# Gear to import dicom files from Google Healthcare API


## Example

```
docker build -t ghc-importer .

$ fw login ...
$ fw gear upload
$ fw ghc login --project healthcare-api-214323 --token "still not used"
$ fw ghc use --location us-central1 --dataset daviddataset --store testdicomstore
$ fw ghc query "PatientSex = \"F\""
Running query...
               StudyInstanceUID                |                   SeriesInstanceUID                    |  StudyDate   |  SeriesDescription  
---------------------------------------------------------------------------------------------------------------------------------------------
  1.2.840.114350.2.277.2.798268.2.287228446.1  |  1.2.840.113619.2.334.3.3599622295.767.1486469021.235  |  2017-02-07  |     Inspiration     
---------------------------------------------------------------------------------------------------------------------------------------------
Query job ID: 49418a97-fcf0-45f5-b4bb-5f4b69055b55

# create a project in core with label `ghc` in `scitran` group
$ fw ghc import 49418a97-fcf0-45f5-b4bb-5f4b69055b55
Starting import...
Job Id: 5baa40cfd5e5460012a1b1bc
$ fw job wait 5baa40cfd5e5460012a1b1bc
Job is running
Job is complete
$ fw ls scitran/ghc
admin Feb  7 10:59 ex 1.2.840.114350.2.277.2.798268.2.287228446.1
```