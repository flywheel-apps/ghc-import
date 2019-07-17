import base64
import datetime
import flywheel
import json
import mock
import pytest

import run

from io import StringIO


PROJECT = {'group': 'scitran', 'label': 'Neuroscience'}

IMPORT_IDS = {
    'dicoms': ['1.2.840.113619.2.243.4560476901969304.96623.9313.6807608'],
    'fhirs': ['Patient/d879d684-892a-4ba7-c4c0-9b68c481635f'],
    'hl7s': ['sXiWf0k3rtURTkhi7144lsgfWgbP41OG-3fv5zvjLtM=']
}

HL7_MESSAGE = {
    'messageType': 'ORU',
    'sendTime': '2018-07-10T06:54:58Z',
    'parsedData': {'segments': [{'segmentId': 'MSH',
                                 'fields': {'9': '51681348'}},
                                {'segmentId': 'PID',
                                 'fields': {'5.1': 'Firstname',
                                            '5.2': 'Lastname',
                                            '7': '19720417',
                                            '8': 'F',
                                            '0': 'PID',
                                            '3.1': 'MRN-ZEN3H',
                                            '3.5': 'MR'}},]
                  },
    'data': 'TZW5kaW5nRmFjfFJlY2VpdmluZ0FwcHxSZWNlaXZpbmdGYWN8MjAxODA3MDcxMzUzMj=',
}

FHIR_RESOURCE_PATIENT = {
    'resourceType': 'Patient',
    'meta': {'lastUpdated': '2019-07-02T13:17:36.759627+0000', 'versionId': 'MTU2McjA3MzQa18H127yNzAwMA'},
    'id': 'be3dce00-0210-4b83-8a00-d479881c821d',
    'gender': 'female',
    'birthDate': '1972-04-17',
    'name': [{'family': 'Lastname', 'given': ['Firstname']}],
    'identifier': [{'type': {'coding': [{'code': 'MR', 'system': 'http://terminology.hl7.org/CodeSystem/v2-0203'}]},
                    'use': 'usual',
                    'value': 'MRN-ZEN3H'}],
}

FHIR_RESOURCE_OBSERVATION = {
    'resourceType': 'Observation',
    'meta': {'lastUpdated': '2019-07-02T13:17:37.265024+0000', 'versionId': 'MTU2McjA3MzQa18H127yNzAwMA'},
    'id': 'fc40ec10-1b29-485d-8cef-7b211e52ebb9',
    'subject': {'reference': 'Patient/be3dce00-0210-4b83-8a00-d479881c821d'},
    'code': {'coding': [{'code': '15074-8', 'display': 'Glucose [Moles/volume] in Blood', 'system': 'http://loinc.org'}]},
    'valueQuantity': {'code': 'mmol/L', 'system': 'http://unitsofmeasure.org', 'unit': 'mmol/l', 'value': 6.3},
}

METADATA_MAP = {
    '/tmp/tmpcrlrrkd3/1.3.46.670589.11.0.0.11.4.2.0.12098.5.7610.1693289264174240079/../1.3.46.670589.11.0.0.11.4.2.0.12098.5.7610.1693289264174240079.dicom.zip':
        {'session':
             {'operator': 'Flywheel^Operator',
              'timestamp': datetime.datetime(2018, 7, 3, 1, 19, 23),
              'uid': '1.2.840.113619.2.243.4814948993375131.82665.1495.9395539',
              'subject':
                  {'code': 'ex3106',
                   'firstname': 'Firstname',
                   'lastname': 'Lastname'}
             },
         'acquisition':
             {'label': 'T1w Structural',
              'timestamp': datetime.datetime(2018, 7, 3, 1, 19, 23),
              'uid': '1.3.46.670589.11.0.0.11.4.2.0.12098.5.7610.1693289264174240079',
              'files': [{'name': '1.3.46.670589.11.0.0.11.4.2.0.12098.5.7610.1693289264174240079.dicom.zip', 'type': 'dicom'}]
             },
         'patient_id': 'MRN-ZEN3H'}
}

CONFIG = {
    'log_level': 'INFO',
    'project_id': 'Neuroscience',
    'auth_token_id': '3Dsg94Af17',
    'hc_dicomstore': 'hc_dicomstore',
    'hc_hl7store': 'hc_hl7store',
    'hc_fhirstore': 'hc_fhirstore',
}

@mock.patch('run.json')
@mock.patch('run.MultipartEncoder')
@mock.patch('run.pkg_series')
@mock.patch('run.search_uids')
@mock.patch('run.get_subject_by_master_code')
@mock.patch('run.get_master_subject_code')
def test_dicom_import(mock_get_master_subject_code, mock_get_subject_by_master_code,
                      mock_search_uids, mock_pkg_series, mock_mpe, mock_json):
    mock_get_master_subject_code.return_value = 'H3B125'
    mock_get_subject_by_master_code.return_value = None
    mock_dicomweb = mock.Mock()
    mock_dicomweb.retrieve_series.return_value = []
    mock_search_uids.return_value = [('1.2.840.113619.2.243.4814948993375131.82665.1495.9395539',
                                      '1.3.46.670589.11.0.0.11.4.2.0.12098.5.7610.1693289264174240079')]
    mock_pkg_series.return_value = METADATA_MAP
    mock_hc_api = mock.Mock()
    mock_hc_api.dicomStores.dicomWeb.return_value = mock_dicomweb
    mock_api = mock.Mock()
    mock_api.post.return_value = mock.Mock()

    with mock.patch('builtins.open', mock.mock_open(read_data=''), create=True) as mock_builtin_open:
        run.import_dicom_files(mock_hc_api, 'hc_dicomstore', IMPORT_IDS['dicoms'], mock_api, PROJECT)
    mock_json.dumps.assert_called_once_with(list(METADATA_MAP.values())[0], default=run.metadata_encoder)
    mock_api.post.assert_called_once()

@mock.patch('run.MultipartEncoder')
@mock.patch('run.get_subject_by_master_code')
@mock.patch('run.get_master_subject_code')
def test_hl7_import(mock_get_master_subject_code, mock_get_subject_by_master_code, MockMultipartEncoder):
    mock_hc_api = mock.Mock()
    msg = run.HL7Message(HL7_MESSAGE)
    assert msg
    mock_get_master_subject_code.return_value = 'H3B125'
    mock_get_subject_by_master_code.return_value = None
    mock_hc_api.hl7V2Stores.messages.get.return_value = HL7_MESSAGE
    mock_api = mock.Mock()
    mock_api.post.return_value = mock.Mock()
    run.import_hl7_messages(mock_hc_api, 'hc_hl7store', IMPORT_IDS['hl7s'], mock_api, PROJECT)
    mock_hc_api.hl7V2Stores.messages.get.assert_called_once_with(name='hc_hl7store/messages/sXiWf0k3rtURTkhi7144lsgfWgbP41OG-3fv5zvjLtM=')
    
    # Extract fields from MultipartEncoder'a args
    fields = MockMultipartEncoder.call_args_list[0][1]['fields']
    assert fields['file'] == (msg.msg_control_id + '.hl7.txt', base64.b64decode(HL7_MESSAGE['data']))

    mock_api.post.assert_called_once()

@mock.patch('run.MultipartEncoder')
@mock.patch('run.get_subject_by_master_code')
@mock.patch('run.get_master_subject_code')
def test_fhir_import(mock_get_master_subject_code, mock_get_subject_by_master_code, MockMultipartEncoder):
    mock_hc_api = mock.Mock()
    mock_hc_api.fhirStores.fhir.read.side_effect = [FHIR_RESOURCE_OBSERVATION, FHIR_RESOURCE_PATIENT]
    mock_get_master_subject_code.return_value = 'H3B125'
    mock_get_subject_by_master_code.return_value = None
    mock_api = mock.Mock()
    mock_api.post.return_value = mock.Mock()
    run.import_fhir_resources(mock_hc_api, 'hc_fhirstore', IMPORT_IDS['fhirs'], mock_api, PROJECT)
    
    # Extract fields and metadata from MultipartEncoder'a args
    fields = MockMultipartEncoder.call_args_list[0][1]['fields']
    assert fields['file'] == ('patient.fhir.json', json.dumps(FHIR_RESOURCE_OBSERVATION, sort_keys=True, 
                                                              indent=4, default=run.metadata_encoder))


    mock_api.post.assert_called_once()

@mock.patch('run.import_hl7_messages')
@mock.patch('run.import_fhir_resources')
@mock.patch('run.import_dicom_files')
@mock.patch('run.HealthcareAPIClient')
@mock.patch('run.FwApi')
def test_main(MockFwApi, MockHcApi, mock_import_dicom_files, mock_import_fhir_resources, mock_import_hl7_messages):
    mock_context = mock.Mock()
    mock_context.configure_mock(config=CONFIG)
    mock_context.get_input.return_value = {'key': 'docker.local.flywheel.io'}
    mock_context.open_input.return_value.__enter__ = lambda *args: StringIO(json.dumps(IMPORT_IDS))
    mock_context.open_input.return_value.__exit__ = lambda *args: None

    hc_api = MockHcApi()
    fw_api = MockFwApi()

    def mock_fw_api_get(*args):
        if 'projects/' + PROJECT['label'] in args:
            return mock.Mock(json=lambda: PROJECT)
        return fw_api

    fw_api.get.side_effect = mock_fw_api_get

    with mock.patch('builtins.open', mock.mock_open(read_data=''), create=True) as mock_builtin_open:
        run.main(mock_context)
    mock_import_dicom_files.assert_called_once_with(hc_api, CONFIG['hc_dicomstore'], IMPORT_IDS['dicoms'],
                                                    fw_api, PROJECT, False)
    mock_import_fhir_resources.assert_called_once_with(hc_api, CONFIG['hc_fhirstore'], IMPORT_IDS['fhirs'],
                                                       fw_api, PROJECT)
    mock_import_hl7_messages.assert_called_once_with(hc_api, CONFIG['hc_hl7store'], IMPORT_IDS['hl7s'],
                                                     fw_api, PROJECT)

def test_get_metadata():
    expected_meta = {'session':
                        {'label': 'HL7_MRN-ZEN3H_2018-07-10',
                         'timestamp': '2018-07-10T06:54:58Z',
                         'subject': 
                             {'code': 'exMRN-ZEN3H',
                              'firstname': 'Firstname',
                              'lastname': 'Lastname',
                              'sex': 'female',
                              'type': 'human'
                              }
                        },
                         'acquisition': 
                             {'label': 'ORU',
                              'timestamp': '2018-07-10T06:54:58Z'
                             }
                    }
    msg = run.HL7Message(HL7_MESSAGE)
    assert msg
    meta = run.get_metadata(msg)
    assert meta == expected_meta
