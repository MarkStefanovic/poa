@ECHO OFF & SETLOCAL
for %%i in ("%~dp0..") DO SET "folder=%%~fi"
@ECHO ON
conda run -n poa --cwd %folder% --live-stream python -m src.main ^
    cleanup ^
    --db dw ^
    --days-to-keep 5
