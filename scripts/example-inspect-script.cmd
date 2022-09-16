@ECHO OFF & SETLOCAL
for %%i in ("%~dp0..") DO SET "folder=%%~fi"
@ECHO ON
conda run -n poa --cwd %folder% --live-stream python -m src.main ^
    inspect ^
    --src-db hh ^
    --cache-db dw ^
    --src-schema opc_prod ^
    --src-table activity ^
    --pk "activity_id" ^
