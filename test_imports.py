import datetime
import json
from io import StringIO
from unittest import mock

import run

PROJECT = {'group': 'scitran', 'label': 'Neuroscience'}

IMPORT_IDS = {
    'dicoms': ['1.2.840.113619.2.243.4560476901969304.96623.9313.6807608'],
    'fhirs': ['Patient/d879d684-892a-4ba7-c4c0-9b68c481635f'],
    'hl7s': ['sXiWf0k3rtURTkhi7144lsgfWgbP41OG-3fv5zvjLtM='],
}

HL7_MESSAGE = {
    'messageType': 'ORU',
    'sendTime': '2018-07-10T06:54:58Z',
    'parsedData': {
        'segments': [
            {
                'segmentId': 'MSH',
                'fields': {
                    '9': '51681348'
                }
            },
            {
                'segmentId': 'PID',
                'fields': {
                    '5.1': 'Firstname',
                    '5.2': 'Lastname',
                    '7': '19720417',
                    '8': 'F',
                    '0': 'PID',
                    '3.1': 'MRN-ZEN3H',
                    '3.5': 'MR',
                },
            },
        ]
    },
    'data':
    'TZW5kaW5nRmFjfFJlY2VpdmluZ0FwcHxSZWNlaXZpbmdGYWN8MjAxODA3MDcxMzUzMj=',
}

FHIR_RESOURCE_PATIENT = {
    'resourceType':
    'Patient',
    'meta': {
        'lastUpdated': '2019-07-02T13:17:36.759627+0000',
        'versionId': 'MTU2McjA3MzQa18H127yNzAwMA',
    },
    'id':
    'be3dce00-0210-4b83-8a00-d479881c821d',
    'gender':
    'female',
    'birthDate':
    '1972-04-17',
    'name': [{
        'family': 'Lastname',
        'given': ['Firstname']
    }],
    'identifier': [{
        'type': {
            'coding': [{
                'code':
                'MR',
                'system':
                'http://terminology.hl7.org/CodeSystem/v2-0203',
            }]
        },
        'use': 'usual',
        'value': 'MRN-ZEN3H',
    }],
}

FHIR_RESOURCE_OBSERVATION = {
    'resourceType': 'Observation',
    'meta': {
        'lastUpdated': '2019-07-02T13:17:37.265024+0000',
        'versionId': 'MTU2McjA3MzQa18H127yNzAwMA',
    },
    'id': 'fc40ec10-1b29-485d-8cef-7b211e52ebb9',
    'subject': {
        'reference': 'Patient/be3dce00-0210-4b83-8a00-d479881c821d'
    },
    'code': {
        'coding': [{
            'code': '15074-8',
            'display': 'Glucose [Moles/volume] in Blood',
            'system': 'http://loinc.org',
        }]
    },
    'valueQuantity': {
        'code': 'mmol/L',
        'system': 'http://unitsofmeasure.org',
        'unit': 'mmol/l',
        'value': 6.3,
    },
}

METADATA_MAP = {
    '/some/path/study_instance_uid.dicom.zip': {
        'session': {
            'label': 'session_label',
            'operator': 'Flywheel^Operator',
            'timestamp': datetime.datetime(2018, 7, 3, 1, 19, 23),
            'uid': '1.2.840.113619.2.243.4814948993375131.82665.1495.9395539',
            'subject': {
                'code': 'ex3106',
                'firstname': 'Firstname',
                'lastname': 'Lastname',
            },
        },
        'acquisition': {
            'label':
            'T1w Structural',
            'timestamp':
            datetime.datetime(2018, 7, 3, 1, 19, 23),
            'uid':
            '1.3.46.670589.11.0.0.11.4.2.0.12098.5.7610.1693289264174240079',
            'files': [{
                'name':
                '1.3.46.670589.11.0.0.11.4.2.0.12098.5.7610.1693289264174240079.dicom.zip',
                'type': 'dicom',
            }],
        },
        'patient_id': 'MRN-ZEN3H',
    }
}

CONFIG = {
    'log_level': 'INFO',
    'project_id': 'Neuroscience',
    'auth_token_id': '3Dsg94Af17',
    'hc_dicomstore': 'hc_dicomstore',
    'hc_hl7store': 'hc_hl7store',
    'hc_fhirstore': 'hc_fhirstore',
}

MOCK_CLIENT_CONFIG = {
    'api_client.call_api.return_value.json.return_value': {
        'access_token': 'token'
    },
    'get_project.return_value': {
        '_id': '000000000000000000000000',
        'group': 'test',
    },
    'get_subject.return_value': {
        '_id': '000000000000000000000000'
    },
    'lookup.return_value': {
        '_id': '000000000000000000000000'
    },
    'get_acquisition.return_value': {
        '_id': '000000000000000000000000'
    }
}


@mock.patch('run.pkg_series')
@mock.patch('run.DicomImporter.search_uids')
@mock.patch('run.Importer._get_subject_by_master_code')
@mock.patch('run.Importer.get_master_subject_code')
@mock.patch('run.HealthcareAPIClient')
def test_dicom_import(
        mock_hc_api,
        mock_get_master_subject_code,
        mock_get_subject_by_master_code,
        mock_search_uids,
        mock_pkg_series,
):
    mock_get_master_subject_code.return_value = 'H3B125'
    mock_get_subject_by_master_code.return_value = None
    mock_dicomweb = mock.Mock()
    mock_dicomweb.retrieve_series.return_value = []
    mock_search_uids.return_value = [(
        '1.2.840.113619.2.243.4814948993375131.82665.1495.9395539',
        '1.3.46.670589.11.0.0.11.4.2.0.12098.5.7610.1693289264174240079',
    )]
    mock_pkg_series.return_value = METADATA_MAP
    mock_hc_api = mock.Mock()
    mock_hc_api.dicomStores.dicomWeb.return_value = mock_dicomweb

    mock_client = mock.Mock()
    mock_client.configure_mock(**MOCK_CLIENT_CONFIG)

    run.DicomImporter(mock_client,
                      CONFIG).import_data(IMPORT_IDS['dicoms'])
    mock_client.upload_file_to_acquisition.assert_called_once_with(
        '000000000000000000000000',
        '/some/path/study_instance_uid.dicom.zip',
        metadata='{}',
    )


@mock.patch('run.Importer._get_subject_by_master_code')
@mock.patch('run.Importer.get_master_subject_code')
@mock.patch('run.HealthcareAPIClient')
def test_hl7_import(
        mock_hc_api,
        mock_get_master_subject_code,
        mock_get_subject_by_master_code
):
    msg = run.HL7Message(HL7_MESSAGE)
    assert msg
    mock_get_master_subject_code.return_value = 'H3B125'
    mock_get_subject_by_master_code.return_value = None
    mock_hc_api_instance = mock.Mock()
    mock_hc_api.return_value = mock_hc_api_instance
    mock_hc_api_instance.hl7V2Stores.messages.get.return_value = HL7_MESSAGE
    mock_client = mock.Mock()
    mock_client.configure_mock(**MOCK_CLIENT_CONFIG)
    run.HL7Importer(
        mock_client, CONFIG).import_data(IMPORT_IDS['hl7s'])
    mock_client.upload_file_to_acquisition.assert_called_once_with(
        '000000000000000000000000',
        mock.ANY,
        metadata=mock.ANY,
    )


@mock.patch('run.Importer._get_subject_by_master_code')
@mock.patch('run.Importer.get_master_subject_code')
@mock.patch('run.HealthcareAPIClient')
def test_fhir_import(
        mock_hc_api,
        mock_get_master_subject_code,
        mock_get_subject_by_master_code,
):
    mock_hc_api_instance = mock.Mock()
    mock_hc_api.return_value = mock_hc_api_instance
    mock_hc_api_instance.fhirStores.fhir.read.return_value = FHIR_RESOURCE_PATIENT
    mock_get_master_subject_code.return_value = 'H3B125'
    mock_get_subject_by_master_code.return_value = None
    mock_client = mock.Mock()
    mock_client.configure_mock(**MOCK_CLIENT_CONFIG)

    run.FHIRImporter(
        mock_client, CONFIG).import_data(IMPORT_IDS['fhirs'])
    mock_client.upload_file_to_subject.assert_called_once_with(
        '000000000000000000000000',
        mock.ANY,
        metadata=mock.ANY,
    )


@mock.patch('run.DicomImporter')
@mock.patch('run.HL7Importer')
@mock.patch('run.FHIRImporter')
@mock.patch('run.HealthcareAPIClient')
def test_main(mock_hc_api, mock_fhir, mock_hl7, mock_dicom):
    mock_context = mock.Mock()
    mock_context.configure_mock(config=CONFIG)
    mock_context.get_input.return_value = {'key': 'docker.local.flywheel.io'}
    mock_context.open_input.return_value.__enter__ = lambda *args: StringIO(
        json.dumps(IMPORT_IDS))
    mock_context.open_input.return_value.__exit__ = lambda *args: None

    mock_dicom_instance = mock.Mock()
    mock_dicom.return_value = mock_dicom_instance

    mock_fhir_instance = mock.Mock()
    mock_fhir.return_value = mock_fhir_instance

    mock_hl7_instance = mock.Mock()
    mock_hl7.return_value = mock_hl7_instance

    with mock.patch('builtins.open', mock.mock_open(), create=True):
        run.main(mock_context)

    mock_dicom_instance.import_data.assert_called_with(
        IMPORT_IDS['dicoms']
    )
    mock_fhir_instance.import_data.assert_called_with(
        IMPORT_IDS['fhirs']
    )
    mock_hl7_instance.import_data.assert_called_with(
        IMPORT_IDS['hl7s']
    )


def test_get_metadata():
    expected_meta = {
        'session': {
            'label': 'HL7_MRN-ZEN3H_2018-07-10',
            'timestamp': '2018-07-10T06:54:58Z',
            'subject': {
                'code': 'exMRN-ZEN3H',
                'firstname': 'Firstname',
                'lastname': 'Lastname',
                'sex': 'female',
                'type': 'human',
            },
        },
        'acquisition': {
            'label': 'ORU',
            'timestamp': '2018-07-10T06:54:58Z'
        },
    }
    msg = run.HL7Message(HL7_MESSAGE)
    assert msg
    meta = run.get_metadata(msg)
    assert meta == expected_meta
