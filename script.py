import datetime
import json
import logging
import os
import pprint
import shutil
import zipfile

from backports import tempfile
from dicomweb_client.api import DICOMwebClient, load_json_dataset
from flywheel_migration.dcm import DicomFile
from flywheel_migration.util import DEFAULT_TZ
import pytz
import requests
from requests_toolbelt.multipart.encoder import MultipartEncoder


log = logging.getLogger('ghc_import')


def main(config_json=None):
    if config_json is None:
        # passing config_json enabled for development - read from file in prod
        config_json = json.load(open('/flywheel/v0/config.json'))
    inputs = config_json['inputs']
    config = config_json['config']
    logging.basicConfig(
        format='%(asctime)s %(name)15.15s %(levelname)4.4s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=getattr(logging, config['log_level']),
    )
    log.debug('config.json\n%s', pprint.pformat(config_json))

    api_key = inputs['key']['key']
    api_uri = api_key.rsplit(':', 1)[0]
    if not api_uri.startswith('http'):
        api_uri = 'https://' + api_uri
    api = requests.session()
    api.headers.update({'Authorization': 'scitran-user ' + api_key})

    dicomweb_token = config.get('token') or api.get(api_uri + '/api/ghc/token').json()['token']
    dicomweb = DICOMwebClient(url=config['dicomweb_uri'], headers={'Authorization': 'Bearer ' + dicomweb_token})

    for uid in config['uids']:
        log.info('Searching for %s %s', config['uid_field'], uid)
        for series in dicomweb.search_for_series(search_filters={str(config['uid_field']): uid}):
            dataset = load_json_dataset(series)
            log.info('Processing %s / %s', dataset.StudyInstanceUID, dataset.SeriesInstanceUID)
            with tempfile.TemporaryDirectory() as tempdir:
                log.debug('  Downloading...')
                dicoms = dicomweb.retrieve_series(dataset.StudyInstanceUID, dataset.SeriesInstanceUID)
                dicom_series = {}
                for dicom in dicoms:
                    dicom_series.setdefault(dicom.SeriesInstanceUID, []).append(dicom)

                log.debug('  Packing...')
                metadata_map = {}
                for series_num, series_uid in enumerate(dicom_series):
                    series_dir = os.path.join(tempdir, series_uid)
                    series = dicom_series[series_uid]
                    os.mkdir(series_dir)
                    for dicom in series:
                        dicom.save_as(os.path.join(series_dir, dicom.SOPInstanceUID))
                    series_meta = pkg_series(series_dir, de_identify=config.get('de_identify', False), timezone=DEFAULT_TZ)
                    metadata_map.update(series_meta)

                log.debug('  Uploading...')
                for filepath, metadata in sorted(metadata_map.iteritems()):
                    metadata.setdefault('group', {})['_id'] = config['group_id']
                    metadata.setdefault('project', {})['label'] = config['project_label']
                    metadata_json = json.dumps(metadata, default=metadata_encoder)

                    filename = os.path.basename(filepath)
                    with open(filepath, 'rb') as f:
                        mpe = MultipartEncoder(fields={'metadata': metadata_json, 'file': (filename, f)})
                        r = api.post(api_uri + '/api/upload/uid', data=mpe, headers={'Content-Type': mpe.content_type})
                    r.raise_for_status()


def pkg_series(path, **kwargs):
    acquisitions = {}
    for filename, filepath in [(filename, os.path.join(path, filename)) for filename in os.listdir(path)]:
        dcm = DicomFile(filepath, parse=True, **kwargs)
        if dcm.acq_no not in acquisitions:
            arcdir_path = os.path.join(path, '..', dcm.acquisition_uid + '.dicom')
            os.mkdir(arcdir_path)
            metadata = get_metadata(dcm)
            acquisitions[dcm.acq_no] = arcdir_path, metadata
        if filename.startswith('(none)'):
            filename = filename.replace('(none)', 'NA')
        file_time = max(int(dcm.acquisition_timestamp.strftime('%s')), 315561600)  # zip can't handle < 1980
        os.utime(filepath, (file_time, file_time))  # correct timestamps
        os.rename(filepath, '%s.dcm' % os.path.join(acquisitions[dcm.acq_no][0], filename))
    metadata_map = {}
    start = datetime.datetime.utcnow()
    for arcdir_path, metadata in acquisitions.itervalues():
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
        group_attrs = [attr for attr in dir(dcm) if attr.startswith(prefix)]
        metadata[group] = {k.replace(prefix, ''): getattr(dcm, k) for k in group_attrs}
    metadata['session']['subject'] = metadata.pop('subject')
    return metadata


def create_archive(content, arcname, metadata=None, outdir=None):
    if hasattr(content, '__iter__'):
        outdir = outdir or os.path.curdir
        files = [(os.path.basename(fp), fp) for fp in content]
    else:
        outdir = outdir or os.path.dirname(content)
        files = [(fn, os.path.join(content, fn)) for fn in os.listdir(content)]
    outpath = os.path.join(outdir, arcname) + '.zip'
    files.sort(key=lambda f: os.path.getsize(f[1]))
    with zipfile.ZipFile(outpath, 'w', zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
        if metadata is not None:
            zf.comment = json.dumps(metadata, default=metadata_encoder)
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
    raise TypeError(repr(obj) + ' is not JSON serializable')


if __name__ == '__main__':
    main()
