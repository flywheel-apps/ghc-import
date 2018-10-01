import datetime
import json
import logging
import os
import shutil
import zipfile

import pytz
import requests
import requests_toolbelt
import tzlocal
from backports import tempfile
from dicomweb_client.api import DICOMwebClient
from flywheel_migration.dcm import DicomFile, DicomFileError

logging.basicConfig(
    format='%(asctime)s %(filename)12.12s %(lineno)5d:%(levelname)5.5s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

log = logging.getLogger()

UPLOAD_ROUTE = '/api/upload/reaper'
FILETYPE = 'dicom'
GEMS_TYPE_SCREENSHOT = ['DERIVED', 'SECONDARY', 'SCREEN SAVE']
GEMS_TYPE_VXTL = ['DERIVED', 'SECONDARY', 'VXTL STATE']
DEFAULT_TZ = tzlocal.get_localzone()
METADATA = [
    # required
    ('group', '_id'),
    ('project', 'label'),
    ('session', 'uid'),
    ('acquisition', 'uid'),
    # desired (for enhanced UI/UX)
    ('session', 'timestamp'),
    ('session', 'timezone'),  # auto-set
    ('subject', 'code'),
    ('acquisition', 'label'),
    ('acquisition', 'timestamp'),
    ('acquisition', 'timezone'),  # auto-set
    ('file', 'type'),
    # optional
    ('session', 'label'),
    ('session', 'operator'),
    ('subject', 'firstname'),
    ('subject', 'lastname'),
    ('subject', 'sex'),
    ('subject', 'age'),
    ('acquisition', 'instrument'),
    ('acquisition', 'measurement'),
    ('file', 'instrument'),
    ('file', 'measurements'),
]


def main():
    with open('/flywheel/v0/config.json') as f:
        config = json.load(f)

    log.setLevel(logging.getLevelName(config['config']['log-level']))

    parts = config['inputs']['key']['key'].split(':')
    api_key = parts[-1]
    api_uri = ':'.join(parts[:-1])
    log.debug('Using API: %s', api_uri)

    if not api_uri.startswith('http'):
        api_uri = 'https://' + api_uri

    session = requests.session()
    session.headers.update({'Authorization': 'scitran-user ' + api_key})

    # get google token for healthcare api requests
    resp = session.get(api_uri + '/api/ghc/token')
    if resp.status_code != 200:
        log.debug(resp.json())
        log.error('Couldn\'t get token from api for GHC')
        exit(1)

    ghc_token = resp.json()['token']

    project_id = config['config']['project']
    location = config['config']['location']
    dataset = config['config']['dataset']
    dicomstore = config['config']['dicomstore']

    log.debug('Using %s/%s/%s/%s' % (project_id, location, dataset, dicomstore))

    client = DICOMwebClient(
        url="https://healthcare.googleapis.com/v1alpha/projects/{project_id}/"
            "locations/{location}/datasets/{dataset}/dicomStores/{dicomstore}"
            "/dicomWeb?access_token={token}".format(
                **{
                    'project_id': project_id,
                    'location': location,
                    'dataset': dataset,
                    'dicomstore': dicomstore,
                    'token': ghc_token
                }
            )
    )

    log.debug('Import level is %s', config['config']['level'])

    for uid in config['config']['uids']:
        with tempfile.TemporaryDirectory() as tempdir:

            if config['config']['level'] == 'series':
                found_series = client.search_for_series(search_filters={
                    'SeriesInstanceUID': uid
                })
            else:
                found_series = client.search_for_series(search_filters={
                    'StudyInstanceUID': uid
                })

            if len(found_series) == 0:
                log.error('No series found to import, %s uid was: %s' % (config['config']['level'], uid))
                exit(1)

            for series in found_series:
                if config['config']['level'] == 'series':
                    state, metadata_map = reap(series['0020000D']['Value'][0], uid, tempdir, client)
                else:
                    state, metadata_map = reap(uid, series['0020000E']['Value'][0], tempdir, client)
                if state == 'reaped':
                    for filepath, metadata in sorted(metadata_map.iteritems()):
                        upload(filepath, metadata, session, api_uri)


def reap(study, series, tempdir, dicom_client):
    log.info('Downloading %s/%s', study, series)
    dicoms = dicom_client.retrieve_series(study, series)

    dicom_series = {}
    for dicom in dicoms:
        dicom_series.setdefault(dicom.SeriesInstanceUID, []).append(dicom)

    series_cnt = len(dicom_series)
    metadata_map = {}
    for series_num, series_uid in enumerate(dicom_series):
        log.info('Reaping      %s (%s/%s)', series_uid, series_num+1, series_cnt)

        reapdir = os.path.join(tempdir, series_uid)
        series = dicom_series[series_uid]
        os.mkdir(reapdir)
        for dicom in series:
            dicom.save_as(os.path.join(reapdir, dicom.SOPInstanceUID))

        log.info('Reaped       %s', series_uid)
        log.info('Processing   %s', series_uid)
        try:
            series_meta = pkg_series(reapdir, de_identify=False, timezone=DEFAULT_TZ)
            metadata_map.update(series_meta)
        except DicomFileError as ex:
            log.error('Invalid dicom: %s', ex)
            return 'unreaped', None

    for meta in metadata_map.itervalues():
        meta['group']['_id'] = 'scitran'
        meta['project']['label'] = 'ghc'
    return 'reaped', metadata_map


def upload(filepath, metadata, http_session, url):
    filename = os.path.basename(filepath)
    log.info('Uploading    %s', filename)
    with open(filepath, 'rb') as fd:
        try:
            start = datetime.datetime.utcnow()
            metadata_json = json.dumps(metadata, default=metadata_encoder)
            mpe = requests_toolbelt.multipart.encoder.MultipartEncoder(fields={'metadata': metadata_json, 'file': (filename, fd)})
            r = http_session.post(url + UPLOAD_ROUTE, data=mpe, headers={'Content-Type': mpe.content_type})
            r.raise_for_status()
            duration = (datetime.datetime.utcnow() - start).total_seconds()
        except requests.ConnectionError as ex:
            log.error('Error        %s: %s', filename, ex)
            return False
        except requests.HTTPError as r:
            log.exception(r)
            return False
    log.info('Uploaded     %s [%s/s]', filename, hrsize(os.path.getsize(filepath) / duration))
    return True


def pkg_series(path, arcname='default', **kwargs):
    # pylint: disable=missing-docstring
    acquisitions = {}
    start = datetime.datetime.utcnow()
    files = [(filename, os.path.join(path, filename)) for filename in os.listdir(path)]
    file_cnt = len(files)
    for filename, filepath in files:
        dcm = DicomFile(filepath, parse=True, **kwargs)
        if dcm.acq_no not in acquisitions:
            if arcname == 'default':
                dir_name = dcm.acquisition_uid
            elif arcname == 'nims':
                dir_name = '{dcm.StudyID}_{dcm.SeriesNumber}_{dcm.AcquisitionNumber}'.format(dcm=dcm.raw)
            else:
                raise ValueError('arcname must be one of ["default", "nims"], got {}'.format(arcname))
            dir_name += '.' + FILETYPE
            arcdir_path = os.path.join(path, '..', dir_name)
            os.mkdir(arcdir_path)
            metadata = object_metadata(dcm, None, timezone=kwargs.get('timezone'))
            acquisitions[dcm.acq_no] = arcdir_path, metadata
        if filename.startswith('(none)'):
            filename = filename.replace('(none)', 'NA')
        file_time = max(int(dcm.acquisition_timestamp.strftime('%s')), 315561600)  # zip can't handle < 1980
        os.utime(filepath, (file_time, file_time))  # correct timestamps
        os.rename(filepath, '%s.dcm' % os.path.join(acquisitions[dcm.acq_no][0], filename))
    duration = (datetime.datetime.utcnow() - start).total_seconds()
    if kwargs.get('de_identify'):
        log.debug('De-id\'ed %d images in %.1fs [%.0f/s]', file_cnt, duration, file_cnt / duration)
    else:
        log.debug('Inspected %d images in %.1fs [%.0f/s]', file_cnt, duration, file_cnt / duration)
    metadata_map = {}
    start = datetime.datetime.utcnow()
    for arcdir_path, metadata in acquisitions.itervalues():
        arc_path = create_archive(arcdir_path, os.path.basename(arcdir_path))
        metadata['acquisition']['files'][0]['name'] = os.path.basename(arc_path)
        set_archive_metadata(arc_path, metadata)
        shutil.rmtree(arcdir_path)
        metadata_map[arc_path] = metadata
    duration = (datetime.datetime.utcnow() - start).total_seconds()
    log.debug('Compressed %d images in %.1fs [%.0f/s]', file_cnt, duration, file_cnt / duration)
    return metadata_map


def create_archive(content, arcname, metadata=None, outdir=None):
    # pylint: disable=missing-docstring
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
    # pylint: disable=missing-docstring
    if isinstance(obj, datetime.datetime):
        if obj.tzinfo is None:
            obj = pytz.timezone('UTC').localize(obj)
        return obj.isoformat()
    elif isinstance(obj, datetime.tzinfo):
        return obj.zone
    raise TypeError(repr(obj) + ' is not JSON serializable')


def set_archive_metadata(path, metadata):
    # pylint: disable=missing-docstring
    with zipfile.ZipFile(path, 'a', zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
        zf.comment = json.dumps(metadata, default=metadata_encoder)


def object_metadata(obj, filename, timezone=None):
    # pylint: disable=missing-docstring
    timezone = DEFAULT_TZ if timezone is None else timezone
    metadata = {
        'file': {},
        'session': {'timezone': timezone},
        'acquisition': {'timezone': timezone},
    }
    for md_group, md_field in METADATA:
        value = getattr(obj, md_group + '_' + md_field, None)
        if value is not None:
            metadata.setdefault(md_group, {})
            metadata[md_group][md_field] = value
    metadata['file']['name'] = filename
    metadata['session']['subject'] = metadata.pop('subject', {})
    metadata['acquisition']['files'] = [metadata.pop('file', {})]
    return metadata


def hrsize(size):
    # pylint: disable=missing-docstring
    if size < 1000:
        return '%d%s' % (size, 'B')
    for suffix in 'KMGTPEZY':
        size /= 1024.
        if size < 10.:
            return '%.1f%sB' % (size, suffix)
        if size < 1000.:
            return '%.0f%sB' % (size, suffix)
    return '%.0f%sB' % (size, 'Y')


if __name__ == '__main__':
    main()
