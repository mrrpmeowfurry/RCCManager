# RCCManager

This is what Nonerev use to manage it's RCCService instances. It starts game servers and renders avatars for PurpRev.

## Build

```bash
go build -o rccmanager ./cmd/rccmanager
```

## Setup

```bash
cp .env.example .env
```

| Variable | What does it do? |
| - | - |
| `RCC_GAME_URL`, `RCC_GAME_SECRET` | The RCCService that runs game servers |
| `RCC_RENDER_URL`, `RCC_RENDER_SECRET` | The one that renders, can be the same one |
| `GAMESERVER_SCRIPT` | Where `gameserver.txt` is |
| `SITE_INTERNAL_URL` | How RCCService reaches the site, no port in it |
| `SITE_PUBLIC_ADDRESS` | What the client connects to |
| `CDN_URL`, `R2_*` | Same as the site |
| `PORT_MIN`, `PORT_MAX` | UDP ports for game servers |
| `LISTEN` | Where the API listens |
| `PASSWORD` | Same as `RCCMANAGER_PASSWORD` on the site |
| `PLAYER_COUNT_TTL` | Player count refresh interval, default `10s` |

> [!NOTE]
> RCCService is ipv4 only, use `127.0.0.1` and not `localhost`.

Then just run it. If something is missing in `.env` it tells you.

## API Reference

Everything needs `Authorization: Bearer <PASSWORD>`.

`GET /servers` and `GET /players` return cached player counts immediately. Counts refresh in the background for ready servers every `PLAYER_COUNT_TTL`; failed refreshes keep the last successful count.

#### Start a server

```http
  POST /servers/${placeId}
```

| Parameter | Type     | Description                        |
| :-------- | :------- | :--------------------------------- |
| `placeId` | `number` | **Required**. Id of place to start |

#### Get all servers

```http
  GET /servers
```

#### Get server

```http
  GET /servers/${jobId}
```

| Parameter | Type     | Description                   |
| :-------- | :------- | :---------------------------- |
| `jobId`   | `string` | **Required**. Id of the server |

#### Close server

```http
  DELETE /servers/${jobId}
```

| Parameter | Type     | Description                    |
| :-------- | :------- | :----------------------------- |
| `jobId`   | `string` | **Required**. Id of the server |

#### Get player counts

```http
  GET /players
```

| Parameter  | Type     | Description                             |
| :--------- | :------- | :-------------------------------------- |
| `placeIds` | `string` | **Required**. Place ids, split by comma |

#### Queue a render

```http
  POST /render/${kind}/${id}
```

| Parameter | Type     | Description                               |
| :-------- | :------- | :---------------------------------------- |
| `kind`    | `string` | **Required**. `avatar`, `place` or `item` |
| `id`      | `number` | **Required**. Id of what to render        |

#### Get render

```http
  GET /render/${kind}/${id}
```

| Parameter | Type     | Description                               |
| :-------- | :------- | :---------------------------------------- |
| `kind`    | `string` | **Required**. `avatar`, `place` or `item` |
| `id`      | `number` | **Required**. Id you queued               |

#### Health

```http
  GET /health
```
