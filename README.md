# ST0263 - Sistemas Distribuidos / Proyecto 1: DFSha

Sistema de archivos distribuido basado en bloques (arquitectura Master-Workers / tipo HDFS) con tolerancia a fallos, consistencia estricta en el plano de control y redundancia en el plano de datos.

## Decisiones Arquitectónicas Principales

### 1. Consenso y Replicación del Plano de Control (Raft)
* Se integró `hashicorp/raft` sobre gRPC para coordinar un clúster de tres nodos de control (`cn1`, `cn2`, `cn3`).
* **Separación de estado duro y blando**: El árbol de directorios y los metadatos de archivos (`fileRegistry`) constituyen el estado duro replicado mediante el log de Raft y persistido en disco usando BoltDB (`raft-boltdb/v2`) y snapshots periódicos. El estado de presencia y métricas de disco de los DataNodes (`nodeRegistry`) se maneja como estado blando local en memoria, alimentado por heartbeats periódicos concurrentes desde cada DataNode.
* **Reenvío transparente (Follower Forwarding)**: Cualquier ControlNode puede recibir peticiones del cliente. Si un nodo follower recibe una lectura o escritura, la redirige al líder actual vía gRPC. Se incluye la cabecera `x-dfsha-forwarded` para prevenir ciclos de reenvío ante transiciones de liderazgo.
* **Consistencia en lectura**: Las operaciones de lectura verifican el liderazgo vigente (`VerifyLeader`) antes de retornar resultados para evitar lecturas estancadas ante particiones de red.

### 2. Plano de Datos y Manejo de Bloques
* **Particionamiento**: Los archivos se dividen en chunks de tamaño fijo (3 MB por defecto).
* **Replicación en cadena (Pipelined Replication)**: La transferencia de bloques se realiza de forma secuencial de DataNode a DataNode para optimizar el ancho de banda de la red del cliente.
* **Integridad**: Cada chunk almacena y valida un hash SHA-256 tanto en tránsito como en reposo (`.checksum`).
* **Autocorrección (Self-Healing)**: El líder monitorea la expiración de DataNodes (umbral de 3 heartbeats perdidos). Al detectar nodos caídos, el líder programa trabajos de replicación entre nodos activos para restablecer el factor de replicación configurado y persiste los nuevos mapeos en Raft.

### 3. Seguridad y Comunicaciones
* **Transporte**: Comunicaciones internas y externas estandarizadas sobre gRPC HTTP/2.
* **Cifrado y Autenticación**: Conexiones protegidas mediante TLS (certificados ECDSA P-256 autogenerados o mediante archivos PEM) e interceptores de autenticación por token Bearer compartido (`AUTH_TOKEN`).

### 4. Tolerancia a Fallos en el Cliente
* El cliente utiliza un canal gRPC configurado con un resolver dinámico multi-endpoint sobre la lista de ControlNodes, utilizando la política `pick_first` y reintentos automáticos ante estados `UNAVAILABLE`, garantizando conmutación transparente durante elecciones de líder.

---

## Estructura del Proyecto

* `cmd/controlnode/`: Punto de entrada del ControlNode, implementación de la FSM de Raft (`fsm.go`), configuración de consenso (`consensus.go`) y proxy de reenvío (`forwarding.go`).
* `cmd/datanode/`: Servidor de bloques de datos y trabajador de heartbeats concurrentes a múltiples ControlNodes.
* `cmd/client/`: CLI para administración y transferencia de archivos (`put`, `get`, `ls`, `mkdir`, `rm`, `rmdir`).
* `pkg/failover/`: Mecanismo de resolución y balanceo multi-nodo para el cliente gRPC.
* `pkg/security/`: Generación de certificados TLS e interceptores de autenticación.
* `pkg/config/`: Utilidades de lectura de variables de entorno y normalización de direcciones.
* `docker-compose.yml`: Definición de infraestructura local con 3 ControlNodes, 3 DataNodes y volúmenes persistentes.

---

## Ejecución del Sistema

### Despliegue con Docker Compose

Iniciar todo el clúster (3 ControlNodes, 3 DataNodes y contenedor de cliente):

```bash
docker compose up -d --build
```

Visualizar estado de los contenedores:

```bash
docker compose ps
```

### Uso del Cliente CLI

El contenedor del cliente incluye un archivo de prueba pregenerado en `/test.bin`.

Subir un archivo a la raíz (`/`):
```bash
docker compose run --rm client put /test.bin
```

Listar contenido de la raíz:
```bash
docker compose run --rm client ls /
```

Crear un directorio y subir un archivo dentro de él (el segundo argumento de `put` es el directorio destino, el cual debe existir previamente):
```bash
docker compose run --rm client mkdir /archivos
docker compose run --rm client put /test.bin /archivos
docker compose run --rm client ls /archivos
```

Descargar un archivo reconstruyendo sus bloques:
```bash
docker compose run --rm client get /archivos/test.bin /tmp/test_recuperado.bin
```

Borrar archivos y directorios:
```bash
docker compose run --rm client rm /archivos/test.bin
docker compose run --rm client rmdir /archivos
```

### Pruebas de Failover del Líder

Para validar la tolerancia a fallos y la persistencia de estado:

1. Detener el contenedor del líder actual (ej. `cn1`):
   ```bash
   docker compose stop cn1
   ```
2. Ejecutar operaciones desde el cliente sin reiniciar dependencias:
   ```bash
   docker compose run --rm --no-deps client ls /
   ```
3. Los nodos restantes convocarán elecciones, seleccionarán un nuevo líder y responderán las solicitudes conservando el estado previo.

