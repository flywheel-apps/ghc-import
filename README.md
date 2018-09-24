# Gear to import dicom files from Google Healthcare API

```
docker build -t ghc-importer .

$ fw login ...
$ fw gear upload
$ fw ghc query test test "PatientSex = \"F\""
2018/09/18 20:38:28 Running big query
Query Job Id: 19e288c9-d412-4dae-be3b-cbf1cdbac36b
+---------------------------------------------+------------------------------------------------------+------------+-------------------+
|              STUDYINSTANCEUID               |                  SERIESINSTANCEUID                   | STUDYDATE  | SERIESDESCRIPTION |
+---------------------------------------------+------------------------------------------------------+------------+-------------------+
| 1.2.840.114350.2.277.2.798268.2.287228446.1 | 1.2.840.113619.2.334.3.3599622295.767.1486469021.235 | 2017-02-07 | Inspiration       |
+---------------------------------------------+------------------------------------------------------+------------+-------------------+


# create a project in core with label `ghc` in `scitran` group
$ fw ghc import 19e288c9-d412-4dae-be3b-cbf1cdbac36b
2018/09/18 20:39:27 Import 19e288c9-d412-4dae-be3b-cbf1cdbac36b job
FW Job Id: 5ba14660e33a0a001224b693
$ fw job wait 5ba14660e33a0a001224b693
Job is running
Job is complete
$ fw ls scitran/ghc
admin Feb  7 10:59 ex 1.2.840.114350.2.277.2.798268.2.287228446.1
```