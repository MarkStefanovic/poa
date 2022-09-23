@ECHO OFF & SETLOCAL
for %%i in ("%~dp0..") DO SET "folder=%%~fi"
@ECHO ON
conda run -n poa --cwd %folder% --live-stream python -m src.main ^
    full-sync ^
    --src-db hh ^
    --src-schema opc_prod ^
    --src-table activity_log_rt ^
    --dst-db dw ^
    --dst-schema hh ^
    --dst-table activity_log_rt ^
    --pk activity_log_id
