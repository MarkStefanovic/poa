@ECHO OFF & SETLOCAL
for %%i in ("%~dp0..") DO SET "folder=%%~fi"
@ECHO ON
%folder%\.\.\dist\poa.exe ^
    incremental-sync ^
    --src-db hh ^
    --dst-db dw ^
    --src-schema opc_prod ^
    --src-table mv_scheduled_activities_rt ^
    --pk "id" ^
    --increasing created_date changed_date ^
