@echo off
"C:\Python27\ArcGIS10.8\python32.exe" "M:\08_Geography\ResponseRateMapper\ReportData\APIDataUpdateAndReportGenerator.py"
IF %ERRORLEVEL% EQU 0 (CALL dashboardupdate)
pause