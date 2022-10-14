@ECHO OFF & SETLOCAL
for %%i in ("%~dp0..") DO SET "folder=%%~fi"
@ECHO ON
conda run -n poa --cwd %folder% --live-stream python -m src.main ^
    incremental-sync ^
    --src-db hh ^
    --src-schema opc_prod ^
    --src-table orbeon_srpepisode_rt ^
    --dst-db dw ^
    --dst-schema hh ^
    --dst-table orbeon_srpepisode_rt ^
    --pk orb_document_id ^
    --compare changed_date
