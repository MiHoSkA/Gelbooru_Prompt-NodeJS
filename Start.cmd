@echo off
TITLE: GelbooruPrompt_APP
chcp 65001 > nul
cd /d "%~dp0"
node --no-warnings server.js
pause