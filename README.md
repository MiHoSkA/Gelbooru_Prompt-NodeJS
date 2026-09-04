# Gelbooru Prompt — Node.js

Веб-приложение на Node.js для получения случайных постов Gelbooru, обработки тегов, просмотра изображений и анимаций и ведения локальной истории.

## Возможности

- Случайный поиск постов по включаемым и исключаемым тегам.
- Загрузка поста по ID или ссылке.
- Обработка тегов перед копированием.
- Локальная история изображений и анимаций.
- Видео не сохраняются в историю.
- Видео отображаются только в основном превью и не открываются в модальном окне.
- Просмотр изображений с увеличением и перемещением.
- Настройки API Gelbooru и прокси.
- Русский и английский интерфейс.
- SQLite с WAL и настройками для быстрой записи.

## Требования

- Node.js 18 или новее.
- npm.
- Доступ к Gelbooru по HTTPS.

Проект рассчитан на Windows, Linux и macOS.

## Установка

```bash
git clone https://github.com/MiHoSkA/Gelbooru_Prompt-NodeJS.git
cd Gelbooru_Prompt-NodeJS
npm install
npm start
```

После запуска откройте `http://localhost:3000`.

## Windows

Откройте PowerShell или командную строку:

```powershell
git clone https://github.com/MiHoSkA/Gelbooru_Prompt-NodeJS.git
cd Gelbooru_Prompt-NodeJS
npm install
npm start
```

Остановить сервер можно сочетанием `Ctrl+C`.

## Linux

```bash
git clone https://github.com/MiHoSkA/Gelbooru_Prompt-NodeJS.git
cd Gelbooru_Prompt-NodeJS
npm install
npm start
```

Для постоянного фонового запуска можно использовать systemd или менеджер процессов Node.js.

## macOS

```bash
git clone https://github.com/MiHoSkA/Gelbooru_Prompt-NodeJS.git
cd Gelbooru_Prompt-NodeJS
npm install
npm start
```

## Переменные окружения

- `PORT` — порт сервера, по умолчанию `3000`.
- `HOST` — адрес прослушивания, по умолчанию `0.0.0.0`.
- `DATA_DIR` — каталог локальных данных, по умолчанию `data`.
- `PUBLIC_DIR` — каталог интерфейса, по умолчанию `public`.

Linux/macOS:

```bash
PORT=3000 HOST=0.0.0.0 npm start
```

PowerShell:

```powershell
$env:PORT="3000"
$env:HOST="0.0.0.0"
npm start
```

## Структура

```text
Gelbooru_Prompt-NodeJS/
├── data/
│   └── favicon.png
├── public/
│   └── index.html
├── .gitignore
├── package.json
├── README.md
└── server.js
```

Каталог `data/` создаётся автоматически. В нём хранится локальная SQLite-база и другие рабочие данные.

## Настройка Gelbooru

API-ключ, ID пользователя и параметры прокси задаются через раздел «Настройки» приложения.

## История

История предназначена только для изображений и анимаций. Сервер создаёт миниатюры изображений и не сохраняет видео в историю. Недопустимые старые записи удаляются автоматически.

## Производительность SQLite

Используются WAL, `synchronous=NORMAL`, `busy_timeout`, увеличенный кэш SQLite и автоматические checkpoint для WAL. История использует уникальность `post_id`, чтобы не создавать дубликаты.

## Безопасность

Административный пароль хранится в `server.js`. Перед размещением приложения в открытом доступе рекомендуется заменить пароль на собственный сложный пароль.

Не публикуйте:

- `data/database.sqlite`;
- `data/`;
- `.env` и другие файлы с секретами;
- API-ключи и пароли.
