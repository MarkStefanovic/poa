@ECHO OFF & SETLOCAL
for %%i in ("%~dp0..") DO SET "folder=%%~fi"
@ECHO ON
conda run -n poa --cwd %folder% --live-stream python -m src.main ^
    check ^
    --src-db hh ^
    --src-schema opc_prod ^
    --src-table activity_detail_dsc_rt ^
    --dst-db dw ^
    --dst-schema hh ^
    --dst-table activity_detail_dsc_rt ^
    --pk "activity_detail_dsc_id"
