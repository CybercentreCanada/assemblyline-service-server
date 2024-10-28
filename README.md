[![Discord](https://img.shields.io/badge/chat-on%20discord-7289da.svg?sanitize=true)](https://discord.gg/GUAy9wErNu)
[![](https://img.shields.io/discord/908084610158714900)](https://discord.gg/GUAy9wErNu)
[![Static Badge](https://img.shields.io/badge/github-assemblyline-blue?logo=github)](https://github.com/CybercentreCanada/assemblyline)
[![Static Badge](https://img.shields.io/badge/github-assemblyline--service--server-blue?logo=github)](https://github.com/CybercentreCanada/assemblyline-service-server)
[![GitHub Issues or Pull Requests by label](https://img.shields.io/github/issues/CybercentreCanada/assemblyline/service-server)](https://github.com/CybercentreCanada/assemblyline/issues?q=is:issue+is:open+label:service-server)
[![License](https://img.shields.io/github/license/CybercentreCanada/assemblyline-service-server)](./LICENSE.md)

# Assemblyline 4 - Service Server

The service server is a API that the service clients can call to interface with the system. This is the only access the services have to the system as they are completely segregated from the other components.

## Image variants and tags

| **Tag Type** | **Description**                                                                                  |      **Example Tag**       |
| :----------: | :----------------------------------------------------------------------------------------------- | :------------------------: |
|    latest    | The most recent build (can be unstable).                                                         |          `latest`          |
|  build_type  | The type of build used. `dev` is the latest unstable build. `stable` is the latest stable build. |     `stable` or `dev`      |
|    series    | Complete build details, including version and build type: `version.buildType`.                   | `4.5.stable`, `4.5.1.dev3` |

## API functionality

Service server provides the following functions via API to the client:

- File download and upload
- Register service to the system
- Get a new task
- Publish results for a task
- Checking if certain tags or files have a reputation relative to the system (ie. safe vs malicious)

#### Running this component

```bash
docker run --name service-server cccs/assemblyline-service-server
```

## Documentation

For more information about this Assemblyline component, follow this [overview](https://cybercentrecanada.github.io/assemblyline4_docs/overview/architecture/) of the system's architecture.

---

# Assemblyline 4 - Serveur de service

Le serveur de services est une API que les clients des services peuvent appeler pour s'interfacer avec le système. C'est le seul accès que les services ont avec le système, car ils sont complètement séparés des autres composants.

## Variantes d'images et balises

| **Type d'étiquette** | **Description**                                                                                                                    |  **Exemple d'étiquette**   |
| :------------------: | :--------------------------------------------------------------------------------------------------------------------------------- | :------------------------: |
|       dernière       | La version la plus récente (peut être instable).                                                                                   |          `latest`          |
|      build_type      | Le type de compilation utilisé. `dev` est la dernière version instable. `stable` est la dernière version stable. |     `stable` ou `dev`      |
|        series        | Le type de build utilisé. `dev` est le dernier build unstable : `version.buildType`.                                               | `4.5.stable`, `4.5.1.dev3` |

## Fonctionnalité de l'API

Le serveur de service fournit les fonctionnalités suivantes au client via l'API :

- Téléchargement et chargement de fichiers
- Enregistrement d'un service dans le système
- Obtenir une nouvelle tâche
- Publier les résultats d'une tâche
- Vérifier si certaines étiquettes ou fichiers ont une réputation liée au système (c'est-à-dire sûrs ou malveillants).

#### Exécuter ce composant

```bash
docker run --name service-server cccs/assemblyline-service-server
```

## Documentation

Pour plus d'informations sur ce composant Assemblyline, suivez ce [overview](https://cybercentrecanada.github.io/assemblyline4_docs/overview/architecture/) de l'architecture du système.
