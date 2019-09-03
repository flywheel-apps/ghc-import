#!/usr/bin/env python3

import base64
import copy
import csv
import datetime
import json
import logging
import os
import pprint
import shutil
import tempfile
import zipfile

import dateutil.parser
import flywheel
import flywheel.rest
import pytz
from dicomweb_client.api import load_json_dataset
from flywheel.file_spec import FileSpec
from flywheel_healthcare_api.client import Client as HealthcareAPIClient
from flywheel_migration.dcm import DicomFile
from flywheel_migration.util import DEFAULT_TZ

logging.basicConfig(level=logging.ERROR)
log = logging.getLogger('ghc_import')

HL7_SEX_MAPPING = {'F': 'female', 'M': 'male', 'O': 'other', 'U': 'unknown'}

HL7_ETHNIC_GROUP_MAP = {
    'H': 'Hispanic or Latino',
    'N': 'Not Hispanic or Latino',
    'U': 'Unknown or Not Reported',
}


def main(context):
    config = context.config
    log.setLevel(getattr(logging, config['log_level']))

    with context.open_input('object_references', 'r') as input_file:
        object_references = json.load(input_file)

    DicomImporter(context.client, context.config).import_data(
        object_references.get('dicoms', [])
    )
    HL7Importer(context.client, context.config).import_data(
        object_references.get('hl7s', [])
    )
    FHIRImporter(context.client, context.config).import_data(
        object_references.get('fhirs', [])
    )


class Importer:
    def __init__(self, fw_client, config):
        self.fw_client = fw_client
        self.fw_api_client = fw_client.api_client
        self.config = config
        self.hc_api = HealthcareAPIClient(self._get_gcp_access_token())
        self.dest_project = self.fw_client.get_project(self.config['project_id'])

    def _get_gcp_access_token(self):
        resp = self.fw_api_client.call_api(
            '/users/self/tokens/' + self.config['auth_token_id'],
            'GET',
            auth_settings=['ApiKey'],
            _preload_content=False,
            _return_http_data_only=True,
        )
        return resp.json()['access_token']

    def _get_subject_by_master_code(self, code):
        subjects = self.fw_client.get_project_subjects(
            self.dest_project['_id'], filter='master_code=' + code
        )

        if len(subjects) > 1:
            raise Exception("Too many matching study, can't decide what to do")
        elif len(subjects) == 1:
            return subjects[0]

        return None

    def create_target_hierarchy(self, metadata):
        subject = self._get_subject_by_master_code(
            metadata['session']['subject']['master_code']
        )
        subject_meta = copy.deepcopy(metadata['session']['subject'])
        for key in ('code', 'firstname', 'lastname', 'sex', 'ethnicity', 'type'):
            if subject and subject.get(key) and subject_meta.get(key):
                # do not overwrite existing fields
                del subject_meta[key]

        if not subject:
            # create the subject if not exists
            subject_meta['project'] = self.dest_project['_id']
            subject_id = self.fw_client.add_subject(body=subject_meta)
            subject = self.fw_client.get_subject(subject_id)
        else:
            # TODO: just update metadata
            pass

        try:
            session = self.fw_client.lookup(
                '{}/<id:{}>/<id:{}>/{}'.format(
                    self.dest_project['group'],
                    self.dest_project['_id'],
                    subject['_id'],
                    metadata['session']['label'],
                )
            )
        except flywheel.rest.ApiException:
            payload = copy.deepcopy(metadata['session'])
            payload['project'] = self.dest_project['_id']
            payload = json.loads(json.dumps(payload, default=metadata_encoder))
            session_id = self.fw_client.add_session(body=payload)
            session = self.fw_client.get_session(session_id)

        if metadata.get('acquisition'):
            try:
                acq = self.fw_client.lookup(
                    '{}/<id:{}>/<id:{}>/{}/{}'.format(
                        self.dest_project['group'],
                        self.dest_project['_id'],
                        subject['_id'],
                        metadata['session']['label'],
                        metadata['acquisition']['label'],
                    )
                )
            except flywheel.rest.ApiException:
                payload = copy.deepcopy(metadata['acquisition'])
                payload['session'] = session['_id']
                if 'files' in payload:
                    del payload['files']
                acq_id = self.fw_client.add_acquisition(body=payload)
                acq = self.fw_client.get_acquisition(acq_id)
        else:
            acq = None

        return {
            'group': self.dest_project['group'],
            'project': self.dest_project['_id'],
            'subject': subject['_id'],
            'session': session['_id'],
            'acquisition': acq['_id'] if acq else None,
        }

    def get_master_subject_code(self, payload):
        log.debug('  Master subject code payload:\n%s', pprint.pformat(payload))
        resp = self.fw_api_client.call_api(
            '/subjects/master-code',
            'POST',
            body=payload,
            auth_settings=['ApiKey'],
            response_type=object,
            _return_http_data_only=True,
        )
        log.debug('  Master subject code response:\n%s', pprint.pformat(resp))
        return resp['code']

    def get_annotations_from_store(self, annotation_store):
        log.info('  Retrieving annotations from {}...'.format(annotation_store))
        # TODO: use self.hc_api once annotation store is available in the same
        # API version than the other store
        hc_api = HealthcareAPIClient(self._get_gcp_access_token(), version='v1alpha2')
        annotations = []
        for annotation_name in hc_api.annotations.list(parent=annotation_store):
            annotation = hc_api.annotations.get(name=annotation_name)
            annotations.append(annotation)
        return annotations

    def import_data(self, *args, **kwargs):
        raise NotImplementedError


class DicomImporter(Importer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dicomweb = self.hc_api.dicomStores.dicomWeb(
            name=self.config.get('hc_dicomstore')
        )

    def search_uids(self, uids):
        series_set = set()
        for uid in uids:
            log.info('  Searching studies and series with UID %s', uid)
            for uid_field in ('StudyInstanceUID', 'SeriesInstanceUID'):
                for series in self.dicomweb.search_for_series(
                    search_filters={uid_field: uid}
                ):
                    dataset = load_json_dataset(series)
                    series_set.add(
                        (dataset.StudyInstanceUID, dataset.SeriesInstanceUID)
                    )
        return sorted(series_set)

    def build_roi_meta(self, annotations, sop_instance_uids):
        roi = []
        annotations = find_annotations_for_instances(annotations, sop_instance_uids)
        for annotation in annotations:
            for bounding_poly in annotation['imageAnnotation']['boundingPolys']:
                r = bounding_poly_to_roi(bounding_poly)
                r['frameIndex'] = 0  # TODO: multiframe?
                r['createdAt'] = datetime.datetime.now()
                # See:
                # https://github.com/flywheel-io/frontend/blob/9078675393b6ca7e67da1b97904c4301860955bf/app/src/common/ohifViewer/ohifViewer.controller.js#L58
                r['studyInstanceUid'] = 'SomeStudyInstanceUid'
                # See:
                # https://github.com/flywheel-io/frontend/blob/9078675393b6ca7e67da1b97904c4301860955bf/app/src/common/ohifViewer/ohifViewer.controller.js#L158
                r['seriesInstanceUid'] = 'RandomSeriesInstanceUid'
                r['sopInstanceUid'] = annotation['sopInstanceUid']
                r['imagePath'] = '{}_{}_{}_0'.format(
                    'SomeStudyInstanceUid',
                    'RandomSeriesInstanceUid',
                    r['sopInstanceUid'],
                )
                r['textBox'] = {
                    'drawnIndependently': True,
                    'allowedOutsideImage': True,
                    'hasBoundingBox': True,
                    'active': False,
                    'hasMoved': False,
                    'movesIndependently': False,
                }

                current_user = self.fw_client.get_current_user()
                r['avatar'] = current_user.get('avatars', {}).get('provider')
                r['updatedAt'] = r['createdAt']
                r['updatedById'] = current_user['email']
                r['updatedByName'] = '{} {}'.format(
                    current_user['firstname'], current_user['lastname']
                )
                r['userId'] = r['updatedById']
                r['userName'] = r['updatedByName']
                roi.append(r)
        return roi

    def import_data(self, *args, **kwargs):
        log.info('Importing DICOM files')
        annotation_store = self.config.get('hc_annotationstore')
        de_identify = self.config.get('de_identify')
        annotations = (
            self.get_annotations_from_store(annotation_store)
            if self.config.get('import_dicom_annotations')
            else None
        )

        if not len(args) == 1:
            raise Exception('One argument is required')

        uids = args[0]
        if not uids:
            log.info('Nothing to import')
            return

        for study_uid, series_uid in self.search_uids(uids):
            log.info('  Processing series %s', series_uid)
            with tempfile.TemporaryDirectory() as tempdir:
                series_dir = os.path.join(tempdir, series_uid)
                os.mkdir(series_dir)
                log.debug('     Downloading...')
                sop_instance_uids = []
                for dicom in self.dicomweb.retrieve_series(study_uid, series_uid):
                    dicom.save_as(os.path.join(series_dir, dicom.SOPInstanceUID))
                    sop_instance_uids.append(dicom.SOPInstanceUID)

                log.debug('     Packing...')
                metadata_map = pkg_series(
                    series_dir,
                    de_identify=de_identify,
                    timezone=DEFAULT_TZ,
                    map_key='PatientID',
                )
                log.debug('     Building ROI metadata...')
                roi_meta = (
                    self.build_roi_meta(annotations, sop_instance_uids)
                    if annotations
                    else {}
                )

                log.debug('     Uploading...')
                for filepath, metadata in sorted(metadata_map.items()):
                    subj_code_payload = {
                        'patient_id': metadata['patient_id'],
                        'use_patient_id': True,
                    }
                    master_subject_code = self.get_master_subject_code(
                        subj_code_payload
                    )
                    metadata['session']['subject']['master_code'] = master_subject_code

                    hierarchy = self.create_target_hierarchy(metadata)

                    filename = os.path.basename(filepath)
                    for r in roi_meta:
                        r[
                            'imageId'
                        ] = '{}/acquisitions/{}/files/{}?member={}.dicom%2F{}.dcm'.format(
                            self.fw_client.api_client.configuration.host.replace(
                                'https', 'dicomweb'
                            ).replace(':443', ''),
                            hierarchy['acquisition'],
                            filename,
                            series_uid,
                            r['sopInstanceUid'],
                        )
                    file_meta = {'info': {'roi': roi_meta}} if roi_meta else {}
                    self.fw_client.upload_file_to_acquisition(
                        hierarchy['acquisition'],
                        filepath,
                        metadata=json.dumps(file_meta, default=metadata_encoder),
                    )


class HL7Importer(Importer):
    def import_data(self, *args, **kwargs):
        log.info('Importing HL7 messages...')
        hl7_store = self.config.get('hc_hl7store')

        if not len(args) == 1:
            raise Exception('One argument is required')

        message_ids = args[0]
        if not message_ids:
            log.info('Nothing to import')
            return

        for msg_id in message_ids:
            log.info('  Processing HL7 message %s', msg_id)
            msg = self.hc_api.hl7V2Stores.messages.get(
                name='{}/messages/{}'.format(hl7_store, msg_id)
            )

            log.debug('     Creating metadata...')
            msg_obj = HL7Message(msg)

            subj_code_payload = {
                'patient_id': msg_obj.patient_id,
                'first_name': msg_obj.subject_firstname,
                'last_name': msg_obj.subject_lastname,
                'date_of_birth': msg_obj.dob.strftime('%Y-%m-%d'),
                'use_patient_id': bool(msg_obj.patient_id),
            }

            master_subject_code = self.get_master_subject_code(subj_code_payload)
            file_meta = normalize_dict_keys(copy.deepcopy(msg))
            del file_meta['data']

            metadata = get_metadata(msg_obj)
            metadata['session']['subject']['master_code'] = master_subject_code
            hierarchy = self.create_target_hierarchy(metadata)

            log.debug('     Uploading...')

            raw_hl7_msg = base64.b64decode(msg['data'])
            file_spec = FileSpec(msg_obj.msg_control_id + '.hl7.txt', raw_hl7_msg)
            file_meta = {'info': file_meta, 'type': 'text'}
            self.fw_client.upload_file_to_acquisition(
                hierarchy['acquisition'],
                file_spec,
                metadata=json.dumps(file_meta, default=metadata_encoder),
            )


class FHIRImporter(Importer):
    def import_data(self, *args, **kwargs):
        log.info('Importing FHIR resources...')
        fhir_store = self.config.get('hc_fhirstore')

        if not len(args) == 1:
            raise Exception('One argument is required')

        fhir_refs = args[0]
        if not fhir_refs:
            log.info('Nothing to import')
            return

        for resource_ref in fhir_refs:
            resource_type, resource_id = resource_ref.split('/')
            resource = self.hc_api.fhirStores.fhir.read(
                name='{}/fhir/{}/{}'.format(fhir_store, resource_type, resource_id)
            )

            log.debug('     Creating metadata...')
            resource_obj = FHIRResource(resource, self.hc_api, fhir_store)

            subj_code_payload = {
                'patient_id': resource_obj.patient_id,
                'first_name': resource_obj.subject_firstname,
                'last_name': resource_obj.subject_lastname,
                'date_of_birth': resource_obj.dob.strftime('%Y-%m-%d'),
                'use_patient_id': bool(resource_obj.patient_id),
            }

            master_subject_code = self.get_master_subject_code(subj_code_payload)

            metadata = get_metadata(resource_obj)
            metadata['session']['subject']['master_code'] = master_subject_code
            collection = (
                'subject'
                if resource_type == 'Patient'
                else 'session'
                if resource_type == 'Encounter'
                else 'acquisition'
            )
            filename = (
                resource_type.lower()
                if resource_type in ['Patient', 'Encounter']
                else resource['id']
            )

            if resource_type in ['Patient', 'Encounter']:
                del metadata['acquisition']

            hierarchy = self.create_target_hierarchy(metadata)
            log.debug('     Uploading...')

            msg_json = json.dumps(
                resource, sort_keys=True, indent=4, default=metadata_encoder
            )

            file_spec = FileSpec(filename + '.fhir.json', msg_json)
            file_meta = {
                'info': {'fhir': resource, **resource_obj.extra_info},
                'type': 'source code',
            }
            getattr(self.fw_client, 'upload_file_to_' + collection)(
                hierarchy[collection],
                file_spec,
                metadata=json.dumps(file_meta, default=metadata_encoder),
            )


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


def pkg_series(path, **kwargs):
    acquisitions = {}
    for filename, filepath in [
        (filename, os.path.join(path, filename)) for filename in os.listdir(path)
    ]:
        dcm = DicomFile(filepath, parse=True, **kwargs)
        if dcm.acq_no not in acquisitions:
            arcdir_path = os.path.join(path, '..', dcm.acquisition_uid + '.dicom')
            os.mkdir(arcdir_path)
            metadata = get_metadata(dcm)
            metadata['patient_id'] = dcm.get('PatientID')
            metadata['session']['label'] = dcm.get('StudyDescription')
            acquisitions[dcm.acq_no] = arcdir_path, metadata
        if filename.startswith('(none)'):
            filename = filename.replace('(none)', 'NA')
        file_time = max(
            int(dcm.acquisition_timestamp.strftime('%s')), 315561600
        )  # zip can't handle < 1980
        os.utime(filepath, (file_time, file_time))  # correct timestamps
        os.rename(
            filepath, '%s.dcm' % os.path.join(acquisitions[dcm.acq_no][0], filename)
        )
    metadata_map = {}
    for arcdir_path, metadata in acquisitions.values():
        arc_name = os.path.basename(arcdir_path)
        metadata['acquisition']['files'] = [
            {'name': arc_name + '.zip', 'type': 'dicom'}
        ]
        arc_path = create_archive(arcdir_path, arc_name, metadata=metadata)
        shutil.rmtree(arcdir_path)
        metadata_map[arc_path] = metadata
    return metadata_map


def get_metadata(dcm):
    metadata = {}
    for group in ('subject', 'session', 'acquisition'):
        prefix = group + '_'
        group_attrs = [
            attr for attr in dir(dcm) if attr.startswith(prefix) and getattr(dcm, attr)
        ]
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

        self.patient_id = (
            pid_segment.get('3') or pid_segment.get('3.1') or pid_segment.get('3[0].1')
        )
        self.subject_code = 'ex' + self.patient_id
        self.subject_firstname = pid_segment.get('5.1')
        self.subject_lastname = pid_segment.get('5.2')
        self.subject_sex = HL7_SEX_MAPPING.get(pid_segment.get('8'))
        self.subject_ethnicity = HL7_ETHNIC_GROUP_MAP.get(pid_segment.get('22'))
        self.subject_type = 'human' if not pid_segment.get('35') else None
        self.dob = datetime.datetime.strptime(pid_segment.get('7'), '%Y%m%d')

        self.session_label = 'HL7_{}_{}'.format(
            self.patient_id,
            datetime.datetime.strptime(
                self.msg_json['sendTime'], '%Y-%m-%dT%H:%M:%SZ'
            ).strftime('%Y-%m-%d'),
        )
        self.session_timestamp = self.acquisition_timestamp = self.msg_json['sendTime']
        self.acquisition_label = self.type

    def get_hl7_segment(self, segment_id):
        for segment in self.segments:
            if segment['segmentId'] == segment_id:
                return segment['fields']

        return None


class FHIRResource:
    def __init__(self, resource, hc_api, hc_fhirstore):
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
                log.warning(
                    '       Subject type %s is not supported yet, SKIPPING',
                    subject_ref.split('/')[0],
                )
            else:
                patient_id = subject_ref.split('/')[1]
                patient = FHIRResource(
                    hc_api.fhirStores.fhir.read(
                        name='{}/fhir/{}/{}'.format(hc_fhirstore, 'Patient', patient_id)
                    ),
                    hc_api,
                    hc_fhirstore,
                )

        self.patient_id = patient.get_id() if patient else None
        self.subject_code = 'ex' + self.patient_id if self.patient_id else None
        self.subject_firstname, self.subject_lastname = (
            patient.get_patient_name() if patient else (None, None)
        )
        self.subject_sex = patient.raw.get('gender') if patient else None

        self.subject_type = (
            (
                'animal'
                if 'http://hl7.org/fhir/StructureDefinition/patient-animal'
                in [e['url'] for e in patient.raw.get('extension', [])]
                else 'human'
            )
            if patient
            else None
        )
        self.dob = (
            datetime.datetime.strptime(patient.raw.get('birthDate'), '%Y-%m-%d')
            if patient
            else None
        )

        self.session_label = 'FHIR_{}_{}'.format(
            self.patient_id, self.last_updated.strftime('%Y-%m-%d')
        )
        self.session_timestamp = self.acquisition_timestamp = self.last_updated
        self.acquisition_label = self.type
        self.extra_info = {}

        # Observation specific section
        if self.type == 'Observation':
            coding = self.raw.get('code', {}).get('coding', [])
            loinc_coding = list(
                filter(lambda coding: coding['system'] == 'http://loinc.org', coding)
            )
            if loinc_coding:
                loinc_coding = loinc_coding[0]
                self.acquisition_label = '{} {}'.format(
                    loinc_coding['code'],
                    # lookup will split worngly the path if the label contains
                    # '/' character
                    loinc_coding['display'].replace('/', '_'),
                )
                loinc_info = self._get_loinc_number_details(loinc_coding['code'])

                if loinc_info:
                    self.extra_info.setdefault('observations', [])
                    if self.raw.get('valueQuantity'):
                        print(loinc_info['SHORTNAME'])
                        self.extra_info['observations'].append(
                            {
                                loinc_info['SHORTNAME']: {
                                    'value': self.raw['valueQuantity']['value'],
                                    'unit': self.raw['valueQuantity']['unit'],
                                    'last_updated': datetime.datetime.strptime(
                                        self.raw['meta']['lastUpdated'],
                                        '%Y-%m-%dT%H:%M:%S.%f%z',
                                    ),
                                }
                            }
                        )

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


def roi_to_bounding_poly(roi):
    bounding_poly = {'vertices': [], 'label': roi['label']}
    if roi['toolType'] == 'freehand':
        for handle in roi['handles']:
            bounding_poly['vertices'].append({'x': handle['x'], 'y': handle['y']})

    return bounding_poly


def bounding_poly_to_roi(bounding_poly):
    roi = {'label': bounding_poly['label'], 'handles': []}
    if len(bounding_poly['vertices']) >= 3:
        roi['toolType'] = 'freehand'
        roi['color'] = '#fbbc05'
        roi['area'] = polygon_area(bounding_poly['vertices'])
        roi['areaInPixels'] = polygon_area(bounding_poly['vertices'])

        top = bounding_poly['vertices'][0]['y']
        left = bounding_poly['vertices'][0]['x']
        for vertex_index in range(len(bounding_poly['vertices'])):
            roi['handles'].append(
                {
                    'x': bounding_poly['vertices'][vertex_index]['x'],
                    'y': bounding_poly['vertices'][vertex_index]['y'],
                    'lines': [],
                }
            )
            if bounding_poly['vertices'][vertex_index]['x'] < left:
                left = bounding_poly['vertices'][vertex_index]['x']
            if bounding_poly['vertices'][vertex_index]['y'] < top:
                left = bounding_poly['vertices'][vertex_index]['y']
            if vertex_index < len(bounding_poly['vertices']) - 1:
                roi['handles'][vertex_index]['lines'].append(
                    {
                        'x': bounding_poly['vertices'][vertex_index + 1]['x'],
                        'y': bounding_poly['vertices'][vertex_index + 1]['y'],
                    }
                )
            else:
                # last handle
                roi['handles'][vertex_index]['lines'].append(roi['handles'][0])
        roi['polyBoundingBox'] = {'height': 50, 'width': 50, 'top': top, 'left': left}
    else:
        raise NotImplementedError

    return roi


def polygon_area(vertices):
    area = 0
    q = vertices[-1]
    for v in vertices:
        area += v['x'] * q['y'] - v['y'] * q['x']
        q = v
    return area / 2


def find_annotations_for_instances(annotations, sop_instance_uids):
    filtered_annotations = []
    for annotation in annotations:
        for sop_instance_uid in sop_instance_uids:
            if sop_instance_uid in annotation['annotationSource'][
                'cloudHealthcareSource'
            ]['name'] and annotation.get('imageAnnotation'):
                annotation['sopInstanceUid'] = sop_instance_uid
                filtered_annotations.append(annotation)

    return filtered_annotations


def enable_docker_local_access(context):
    """Enable accessing docker.local.flywheel.io within a gear (ie. in development)"""
    if 'docker.local.flywheel.io' in context.get_input('key').get('key', ''):
        if os.path.exists('docker_host'):
            docker_host = open('docker_host').read().strip()
            with open('/etc/hosts', 'a') as hosts:
                hosts.write(docker_host + '\tdocker.local.flywheel.io\n')
        else:
            cmd = "ip -o route get to 8.8.8.8 | sed 's/^.*src \([^ ]*\).*$/\1/;q' > docker_host"
            log.warning(
                'cannot patch /etc/hosts with docker.local.flywheel.io - docker_host file not found. '
                "Run the following command to create the file in your gear's root dir: \n%s",
                cmd,
            )


if __name__ == '__main__':
    with flywheel.GearContext() as context:
        enable_docker_local_access(context)
        context.init_logging()
        context.log_config()
        main(context)
