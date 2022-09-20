@ECHO OFF & SETLOCAL
for %%i in ("%~dp0..") DO SET "folder=%%~fi"
@ECHO ON
conda run -n poa --cwd %folder% --live-stream python -m src.main ^
    incremental-sync ^
    --src-db hh ^
    --src-schema opc_prod ^
    --src-table activity_rt ^
    --dst-db dw ^
    --dst-schema dw ^
    --dst-table activity_rt ^
    --pk "activity_id" ^
    --compare created_date changed_date ^
