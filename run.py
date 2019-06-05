import base64
import copy
import csv
import datetime
import json
import logging
import os
import pprint
import shutil
import sys
import tempfile
import zipfile
import re

import dateutil.parser
from healthcare_api.client import Client as HealthcareAPIClient
import pytz
import requests
from dicomweb_client.api import load_json_dataset
from flywheel_migration.dcm import DicomFile
from flywheel_migration.util import DEFAULT_TZ
from requests_toolbelt.multipart.encoder import MultipartEncoder

log = logging.getLogger('ghc_import')


HL7_SEX_MAPPING = {
    'F': 'female',
    'M': 'male',
    'O': 'other',
    'U': 'unknown'
}

HL7_ETHNIC_GROUP_MAP = {
    'H': 'Hispanic or Latino',
    'N': 'Not Hispanic or Latino',
    'U': 'Unknown or Not Reported',
}


def main(config_json=None):
    if config_json is None:
        # passing config_json enabled for development - read from file in prod
        config_json = json.load(open('/flywheel/v0/config.json'))
    inputs = config_json['inputs']
    config = config_json['config']
    logging.basicConfig(
        format='%(asctime)s %(name)15.15s %(levelname)4.4s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.ERROR,
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    log.setLevel(getattr(logging, config['log_level']))

    log.debug('config.json\n%s', pprint.pformat(config_json))

    api_key = inputs['key']['key']

    if 'docker.local.flywheel.io' in api_key:
        # dev workaround for accessing docker.local - set own host ip
        # ip -o route get to 8.8.8.8 | sed -n 's/.*src \([0-9.]\+\).*/\1/p'
        host_ip = '192.168.50.189'
        with open('/etc/hosts', 'a') as f:
            f.write(host_ip + '\tdocker.local.flywheel.io\n')

    api_uri = api_key.rsplit(':', 1)[0]
    if not api_uri.startswith('http'):
        api_uri = 'https://' + api_uri + '/api'
    api = requests.session()
    api.headers.update({'Authorization': 'scitran-user ' + api_key})

    # validate destination project exists
    resp = api.get(api_uri + '/projects/' + config['project_id'])
    resp.raise_for_status()
    proj = resp.json()

    resp = api.get(api_uri + '/users/self/tokens/' + config['auth_token_id'])
    resp.raise_for_status()
    access_token = resp.json()['access_token']

    if config.get('uids'):
        import_dicom_files(access_token, config, proj, api, api_uri)

    if config.get('hl7_msg_ids'):
        import_hl7_messages(access_token, config, proj, api, api_uri)

    if config.get('fhir_resource_refs'):
        import_fhir_resources(access_token, config, proj, api, api_uri)


def import_dicom_files(access_token, config, project, api, api_uri):
    log.info('Importing DICOM files...')
    hc_api = HealthcareAPIClient(access_token)
    dicomweb = hc_api.get_dicomweb_client(config['hc_dicomstore'])

    for study_uid, series_uid in search_uids(dicomweb, config['uids']):
        log.info('  Processing series %s', series_uid)
        with tempfile.TemporaryDirectory() as tempdir:
            series_dir = os.path.join(tempdir, series_uid)
            os.mkdir(series_dir)
            log.debug('     Downloading...')
            for dicom in dicomweb.retrieve_series(study_uid, series_uid):
                dicom.save_as(os.path.join(series_dir, dicom.SOPInstanceUID))

            log.debug('     Packing...')
            metadata_map = pkg_series(series_dir, de_identify=config.get('de_identify', False), timezone=DEFAULT_TZ, map_key='PatientID')

            log.debug('     Uploading...')
            for filepath, metadata in sorted(metadata_map.items()):
                subj_code_payload = {
                    'patient_id': metadata['patient_id'],
                    'use_patient_id': True
                }
                del metadata['patient_id']
                master_subject_code = get_master_subject_code(subj_code_payload, api, api_uri)
                subject = get_subject_by_master_code(master_subject_code, project, api, api_uri)

                metadata.setdefault('group', {})['_id'] = project['group']
                metadata.setdefault('project', {})['label'] = project['label']
                subject_info = copy.deepcopy(metadata['session']['subject'])
                metadata['session']['subject'] = {'master_code': master_subject_code}

                for key in ('code', 'firstname', 'lastname'):
                    if not (subject and subject.get(key)) and subject_info.get(key):
                        metadata['session']['subject'][key] = subject_info[key]

                metadata_json = json.dumps(metadata, default=metadata_encoder)

                filename = os.path.basename(filepath)
                with open(filepath, 'rb') as f:
                    mpe = MultipartEncoder(fields={'metadata': metadata_json, 'file': (filename, f)})
                    resp = api.post(api_uri + '/upload/uid', data=mpe, headers={'Content-Type': mpe.content_type})
                    resp.raise_for_status()


def import_hl7_messages(access_token, config, project, api, api_uri):
    log.info('Importing HL7 messages...')
    hc_api = HealthcareAPIClient(access_token)

    for msg_id in config['hl7_msg_ids']:
        log.info('  Processing HL7 message %s', msg_id)
        msg = hc_api.get_hl7v2_message('{}/messages/{}'.format(config['hc_hl7store'], msg_id))

        log.debug('     Creating metadata...')
        msg_obj = HL7Message(msg)

        subj_code_payload = {
            'patient_id': msg_obj.patient_id,
            'first_name': msg_obj.subject_firstname,
            'last_name': msg_obj.subject_lastname,
            'date_of_birth': msg_obj.dob.strftime('%Y-%m-%d'),
            'use_patient_id': bool(msg_obj.patient_id)
        }

        master_subject_code = get_master_subject_code(subj_code_payload, api, api_uri)
        subject = get_subject_by_master_code(master_subject_code, project, api, api_uri)

        file_meta = normalize_dict_keys(copy.deepcopy(msg))
        del file_meta['data']

        metadata = get_metadata(msg_obj)
        metadata.setdefault('group', {})['_id'] = project['group']
        metadata.setdefault('project', {})['label'] = project['label']
        subject_info = copy.deepcopy(metadata['session']['subject'])
        metadata['session']['subject'] = {'master_code': master_subject_code}
        metadata['acquisition']['files'] = [
            {
                'name': msg_obj.msg_control_id + '.hl7.txt',
                'type': 'hl7',
                'info': file_meta
            }
        ]

        for key in ('code', 'firstname', 'lastname', 'sex', 'ethnicity', 'type'):
            if not (subject and subject.get(key)) and subject_info.get(key):
                metadata['session']['subject'][key] = subject_info[key]

        log.debug('     Upload metadata:\n%s', pprint.pformat(metadata))
        log.debug('     Uploading...')

        metadata_json = json.dumps(metadata, default=metadata_encoder)
        raw_hl7_msg = base64.b64decode(msg['data'])
        mpe = MultipartEncoder(fields={'metadata': metadata_json, 'file': (msg_obj.msg_control_id + '.hl7.txt', raw_hl7_msg)})
        resp = api.post(api_uri + '/upload/label', data=mpe, headers={'Content-Type': mpe.content_type})
        log.debug('     Upload response:\n%s', pprint.pformat(resp.json()))
        resp.raise_for_status()


def import_fhir_resources(access_token, config, project, api, api_uri):
    log.info('Importing FHIR resources...')
    hc_api = HealthcareAPIClient(access_token)

    for resource_ref in config['fhir_resource_refs']:
        resource_type, resource_id = resource_ref.split('/')
        resource = hc_api.read_fhir_resource('{}/fhir/{}/{}'.format(config['hc_fhirstore'], resource_type, resource_id))

        log.debug('     Creating metadata...')
        resource_obj = FHIRResource(resource, hc_api, config)

        subj_code_payload = {
            'patient_id': resource_obj.patient_id,
            'first_name': resource_obj.subject_firstname,
            'last_name': resource_obj.subject_lastname,
            'date_of_birth': resource_obj.dob.strftime('%Y-%m-%d'),
            'use_patient_id': bool(resource_obj.patient_id)
        }

        master_subject_code = get_master_subject_code(subj_code_payload, api, api_uri)

        log.debug(master_subject_code)
        subject = get_subject_by_master_code(master_subject_code, project, api, api_uri)

        metadata = get_metadata(resource_obj)
        metadata.setdefault('group', {})['_id'] = project['group']
        metadata.setdefault('project', {})['label'] = project['label']
        subject_info = copy.deepcopy(metadata['session']['subject'])
        metadata['session']['subject'] = {'master_code': master_subject_code}
        collection = metadata['session']['subject'] if resource_type == 'Patient' else metadata['session'] if resource_type == 'Encounter' else metadata['acquisition']
        filename = resource_type.lower() if resource_type in ['Patient', 'Encounter'] else resource['id']
        collection['files'] = [
            {
                'name': filename + '.fhir.json',
                'type': 'fhir',
                'info': {'fhir': resource, **resource_obj.extra_info}
            }
        ]

        if resource_type in ['Patient', 'Encounter']:
            del metadata['acquisition']

        for key in ('code', 'firstname', 'lastname', 'sex', 'type'):
            if not (subject and subject.get(key)) and subject_info.get(key):
                metadata['session']['subject'][key] = subject_info[key]

        log.debug('     Upload metadata:\n%s', pprint.pformat(metadata))
        log.debug('     Uploading...')

        metadata_json = json.dumps(metadata, default=metadata_encoder)
        msg_json = json.dumps(resource, sort_keys=True, indent=4, default=metadata_encoder)
        mpe = MultipartEncoder(fields={'metadata': metadata_json, 'file': (filename + '.fhir.json', msg_json)})
        resp = api.post(api_uri + '/upload/label', data=mpe, headers={'Content-Type': mpe.content_type})
        log.debug('     Upload response:\n%s', pprint.pformat(resp.json()))
        resp.raise_for_status()


def get_master_subject_code(payload, api, api_uri):
    payload_json = json.dumps(payload, default=metadata_encoder)
    log.debug('  Master subject code payload:\n%s', pprint.pformat(payload))
    resp = api.post(api_uri + '/subjects/master-code', data=payload_json)
    log.debug('  Master subject code response:\n%s', pprint.pformat(resp.json()))
    resp.raise_for_status()
    return resp.json()['code']


def get_subject_by_master_code(code, project, api, api_uri):
    resp = api.get(api_uri + '/subjects')
    resp.raise_for_status()
    matching_subjects = [s for s in resp.json() if s.get('master_code') == code and s['project'] == project['_id']]

    if len(matching_subjects) > 1:
        raise Exception("Too many matching study, can't decide what to do")
    elif len(matching_subjects) == 1:
        return matching_subjects[0]

    return None


def normalize_dict_keys(d):
    new = {}
    for k, v in d.items():
        if isinstance(v, dict):
            v = normalize_dict_keys(v)
        elif isinstance(v, list):
            sub_list = []
            for i in v:
                sub_list.append(normalize_dict_keys(i))
            v = sub_list

        new[k.replace('.', '_')] = v
    return new


def search_uids(dicomweb, uids):
    series_set = set()
    for uid in uids:
        log.info('  Searching studies and series with UID %s', uid)
        for uid_field in ('StudyInstanceUID', 'SeriesInstanceUID'):
            for series in dicomweb.search_for_series(search_filters={uid_field: uid}):
                dataset = load_json_dataset(series)
                series_set.add((dataset.StudyInstanceUID, dataset.SeriesInstanceUID))
    return sorted(series_set)


def pkg_series(path, **kwargs):
    acquisitions = {}
    for filename, filepath in [(filename, os.path.join(path, filename)) for filename in os.listdir(path)]:
        dcm = DicomFile(filepath, parse=True, **kwargs)
        if dcm.acq_no not in acquisitions:
            arcdir_path = os.path.join(path, '..', dcm.acquisition_uid + '.dicom')
            os.mkdir(arcdir_path)
            metadata = get_metadata(dcm)
            metadata['patient_id'] = dcm.get('PatientID')
            acquisitions[dcm.acq_no] = arcdir_path, metadata
        if filename.startswith('(none)'):
            filename = filename.replace('(none)', 'NA')
        file_time = max(int(dcm.acquisition_timestamp.strftime('%s')), 315561600)  # zip can't handle < 1980
        os.utime(filepath, (file_time, file_time))  # correct timestamps
        os.rename(filepath, '%s.dcm' % os.path.join(acquisitions[dcm.acq_no][0], filename))
    metadata_map = {}
    for arcdir_path, metadata in acquisitions.values():
        arc_name = os.path.basename(arcdir_path)
        metadata['acquisition']['files'] = [{'name': arc_name + '.zip', 'type': 'dicom'}]
        arc_path = create_archive(arcdir_path, arc_name, metadata=metadata)
        shutil.rmtree(arcdir_path)
        metadata_map[arc_path] = metadata
    return metadata_map


def get_metadata(dcm):
    metadata = {}
    for group in ('subject', 'session', 'acquisition'):
        prefix = group + '_'
        group_attrs = [attr for attr in dir(dcm) if attr.startswith(prefix) and getattr(dcm, attr)]
        metadata[group] = {k.replace(prefix, ''): getattr(dcm, k) for k in group_attrs}
    metadata['session']['subject'] = metadata.pop('subject')
    return metadata


def create_archive(content, arcname, metadata=None, outdir=None):
    outdir = outdir or os.path.dirname(content)
    files = [(fn, os.path.join(content, fn)) for fn in os.listdir(content)]
    outpath = os.path.join(outdir, arcname) + '.zip'
    files.sort(key=lambda f: os.path.getsize(f[1]))
    with zipfile.ZipFile(outpath, 'w', zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
        if metadata is not None:
            zf.comment = json.dumps(metadata, default=metadata_encoder).encode('utf-8')
        for fn, fp in files:
            zf.write(fp, os.path.join(arcname, fn))
    return outpath


def metadata_encoder(obj):
    if isinstance(obj, datetime.datetime):
        if obj.tzinfo is None:
            obj = pytz.timezone('UTC').localize(obj)
        return obj.isoformat()
    elif isinstance(obj, datetime.tzinfo):
        return obj.zone
    elif hasattr(obj, 'encode'):
        return str(obj.encode())

    raise TypeError(repr(obj) + ' is not JSON serializable')


class HL7Message:
    def __init__(self, hc_api_msg):
        self.msg_json = hc_api_msg
        self.segments = self.msg_json['parsedData']['segments']
        self.msg_control_id = self.segments[0]['fields']['9']
        self.type = self.msg_json['messageType']

        pid_segment = self.get_hl7_segment('PID')

        self.patient_id = pid_segment.get('3') or pid_segment.get('3.1') or pid_segment.get('3[0].1')
        self.subject_code = 'ex' + self.patient_id
        self.subject_firstname = pid_segment.get('5.1')
        self.subject_lastname = pid_segment.get('5.2')
        self.subject_sex = HL7_SEX_MAPPING.get(pid_segment.get('8'))
        self.subject_ethnicity = HL7_ETHNIC_GROUP_MAP.get(pid_segment.get('22'))
        self.subject_type = 'human' if not pid_segment.get('35') else None
        self.dob = datetime.datetime.strptime(pid_segment.get('7'), '%Y%m%d')

        self.session_label = 'HL7_{}_{}'.format(
            self.patient_id,
            datetime.datetime.strptime(self.msg_json['sendTime'],
                                       '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d')
        )
        self.session_timestamp = self.acquisition_timestamp = self.msg_json['sendTime']
        self.acquisition_label = self.type

    def get_hl7_segment(self, segment_id):
        for segment in self.segments:
            if segment['segmentId'] == segment_id:
                return segment['fields']

        return None


class FHIRResource:
    def __init__(self, resource, hc_api, config):
        self.raw = resource
        self.type = self.raw['resourceType']
        self.last_updated = dateutil.parser.parse(self.raw['meta']['lastUpdated'])

        patient = None
        if self.type == 'Patient':
            patient = self
        else:
            subject_ref = None
            if self.raw.get('patient', {}).get('reference'):
                subject_ref = self.raw.get('patient', {}).get('reference')

            if self.raw.get('subject', {}).get('reference'):
                subject_ref = self.raw.get('subject', {}).get('reference')

            if not subject_ref:
                log.warning('       No subject found, SKIPPING')
            elif not subject_ref.startswith('Patient/'):
                log.warning('       Subject type %s is not supported yet, SKIPPING', subject_ref.split('/')[0])
            else:
                patient_id = subject_ref.split('/')[1]
                patient = FHIRResource(
                    hc_api.read_fhir_resource('{}/fhir/{}/{}'.format(config['hc_fhirstore'], 'Patient', patient_id)),
                    hc_api,
                    config
                )

        self.patient_id = patient.get_id() if patient else None
        self.subject_code = 'ex' + self.patient_id if self.patient_id else None
        self.subject_firstname, self.subject_lastname = patient.get_patient_name() if patient else (None, None)
        self.subject_sex = patient.raw.get('gender') if patient else None

        self.subject_type = ('animal' if 'http://hl7.org/fhir/StructureDefinition/patient-animal' in
                             [e['url'] for e in patient.raw.get('extension', [])] else 'human') if patient else None
        self.dob = datetime.datetime.strptime(patient.raw.get('birthDate'), '%Y-%m-%d') if patient else None

        self.session_label = 'FHIR_{}_{}'.format(self.patient_id, self.last_updated.strftime('%Y-%m-%d'))
        self.session_timestamp = self.acquisition_timestamp = self.last_updated
        self.acquisition_label = self.type
        self.extra_info = {}

        # Observation specific section
        if self.type == 'Observation':
            coding = self.raw.get('code', {}).get('coding', [])
            loinc_coding = list(filter(lambda coding: coding['system'] == 'http://loinc.org', coding))
            if loinc_coding:
                loinc_coding = loinc_coding[0]
                self.acquisition_label = '{} {}'.format(
                    loinc_coding['code'],
                    loinc_coding['display']
                )
                loinc_info = self._get_loinc_number_details(loinc_coding['code'])

                if loinc_info:
                    self.extra_info.setdefault('observations', [])
                    if self.raw.get('valueQuantity'):
                        print(loinc_info['SHORTNAME'])
                        self.extra_info['observations'].append({
                            loinc_info['SHORTNAME']: {
                                'value': self.raw['valueQuantity']['value'],
                                'unit': self.raw['valueQuantity']['unit'],
                                'last_updated': datetime.datetime.strptime(self.raw['meta']['lastUpdated'], '%Y-%m-%dT%H:%M:%S.%f%z')
                            }
                        })
    def get_id(self, fallback_to_id=False):
        _id = None
        if self.raw.get('identifier'):
            _id = self.raw['identifier'][0]['value']

        if not _id and fallback_to_id:
            _id = self.raw['id']

        return _id

    def get_patient_name(self):
        first_name = None
        last_name = None

        if self.raw.get('name'):
            first_name = ' '.join(self.raw['name'][0].get('given', [])).strip()
            last_name = self.raw['name'][0].get('family')

        return first_name, last_name

    def _get_loinc_number_details(self, loinc_number):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(dir_path, 'LoincTableCore.csv'), newline='') as csvfile:
            dialect = csv.Sniffer().sniff(csvfile.read(1024))
            csvfile.seek(0)
            reader = csv.DictReader(csvfile, dialect=dialect)
            for row in reader:
                if row['LOINC_NUM'] == loinc_number:
                    return row
            return None


if __name__ == '__main__':
    main()
