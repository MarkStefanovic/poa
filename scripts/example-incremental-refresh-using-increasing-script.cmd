@ECHO OFF & SETLOCAL
for %%i in ("%~dp0..") DO SET "folder=%%~fi"
@ECHO ON
conda run -n poa --cwd %folder% --live-stream python -m src.main ^
    incremental-sync ^
    --src-db dt ^
    --src-schema dbo ^
    --src-table codetables ^
    --dst-db dw ^
    --dst-schema dt ^
    --dst-table codetables ^
    --pk code ^
    --increasing datecreated lastupdated ^
    --track-history
