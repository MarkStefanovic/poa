@ECHO OFF & SETLOCAL
for %%i in ("%~dp0..") DO SET "folder=%%~fi"
@ECHO ON
%folder%\.\.\dist\poa.exe ^
    incremental-sync ^
    --src-db hh ^
    --src-schema opc_prod ^
    --src-table mv_scheduled_activities_rt ^
    --dst-db dw ^
    --dst-schema hh ^
    --pk "id" ^
    --increasing created_date changed_date ^
