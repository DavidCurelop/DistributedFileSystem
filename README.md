# ST0263 - Sistemas Distribuidos / Proyecto 1: DFSha

Sistema de archivos distribuido basado en bloques (arquitectura Master-Workers con consenso Raft) tolerante a fallos, con cifrado en reposo, control de acceso por usuario y consistencia estricta en el plano de control.

---

## 1. Arquitectura del Sistema

El sistema implementa una arquitectura desacoplada entre el plano de control y el plano de datos:

* **ControlNode (Plano de Control)**: Coordina el espacio de nombres jerárquico POSIX (`fileRegistry`), mapeo de bloques a nodos y registro de DataNodes (`nodeRegistry`). Utiliza una máquina de estados replicada mediante el protocolo de consenso Raft (`hashicorp/raft`) sobre gRPC y almacenamiento BoltDB. Implementa failover automático de líder, redirección transparente desde seguidores (`x-dfsha-forwarded`) y monitor de auto-recuperación (self-healing).
* **DataNode (Plano de Datos)**: Almacena físicamente los bloques de datos. Aplica cifrado en reposo AES-256-GCM, verifica sumas de verificación SHA-256, reporta estado mediante heartbeats periódicos a los ControlNodes y participa en replicación encadenada.
* **Cliente CLI**: Interfaz interactiva de usuario y línea de comandos para transferir archivos y gestionar directorios. Mantiene estado de directorio de trabajo local (`cwd`), resuelve rutas relativas/absolutas y transmite credenciales de usuario y autenticación mediante metadatos gRPC.

---

## 2. Protocolos de Comunicación gRPC

La interacción entre nodos se distribuye en cinco protocolos estandarizados sobre HTTP/2 y Protocol Buffers:

1. **Cliente <-> ControlNode**: Gestión de metadatos de archivos y directorios (`FileMetadataService`: `CreateFileEntry`, `GetFileMetadata`, `ListDirectory`, `CreateDirectory`, `DeleteFile`, `DeleteDirectory`) y asignación de ubicaciones de bloques (`FileSaving`: `RequestNodeIPs`).
2. **Cliente <-> DataNode**: Transmisión y almacenamiento directo de bloques de datos (`DataNodeService`: `StoreChunk`, `GetChunk`). El cliente no almacena datos en el ControlNode.
3. **DataNode <-> DataNode**: Replicación encadenada (*pipelined replication*) de bloques. El DataNode primario recibe el chunk y la lista de réplicas restantes, propagando el payload al siguiente nodo antes de confirmar la escritura.
4. **ControlNode <-> DataNode**: Registro inicial (`RegisterNode`) y heartbeats periódicos concurrentes (`Heartbeat`). El ControlNode líder monitorea expiración de nodos y emite transferencias de self-healing cuando el factor de replicación disminuye.
5. **ControlNode <-> ControlNode**: Consenso distribuido Raft (`RaftService`: `AppendEntries`, `RequestVote`, `InstallSnapshot`) y proxy de solicitudes hacia el líder vigente con propagación de contexto y usuario.

---

## 3. Cumplimiento de Requerimientos del Sistema

* **RF1 (Interfaz y CLI)**:
  * Directorio de trabajo local con comandos `pwd` y `cd <ruta>`. Soporta rutas relativas (`.`, `..`, subdirectorios) y absolutas (`/`).
  * Validación previa de existencia de directorios remotos antes de modificar `cwd`.
  * Prompt interactivo dinámico con formato `dfsha:<cwd>> `.
  * Comandos soportados: `put`, `get`, `ls`, `cd`, `pwd`, `mkdir`, `rm`, `rmdir`, `exit`.
* **RF2 (Manejo de Archivos y Particionamiento)**:
  * División de archivos en chunks de 3 MB (`defaultPartitionSize`).
  * Integridad garantizada mediante hash SHA-256 por cada bloque.
  * Replicación pipelined en DataNodes con factor de replicación configurable.
* **RF3 (Tolerancia a Fallos y Alta Disponibilidad)**:
  * Clúster multi-ControlNode (3 nodos) con quórum estricto y elección automática ante desconexión del líder.
  * Replicación de estado del árbol de archivos persistido en log de Raft y snapshots.
  * Failover en cliente con reintentos transparentes ante código `UNAVAILABLE`.
  * Auto-recuperación (self-healing) en segundo plano ante pérdida de DataNodes.

---

## 4. Arquitectura de Seguridad

* **Transporte Seguro (TLS 1.2+)**: Canales gRPC asegurados mediante TLS utilizando claves criptográficas ECDSA P-256. Soporta certificados provistos por archivo CA o autogeneración en tiempo de arranque.
* **Autenticación por Token**: Interceptores unarios y de streaming que exigen `authorization: Bearer <token>` o cabecera `x-auth-token`.
* **Cifrado en Reposo (AES-256-GCM)**: Implementado en `pkg/security/crypto.go`. La clave simétrica de 256 bits se deriva de la frase de paso usando `sha256.Sum256`. Cada bloque genera un nonce criptográfico de 12 bytes mediante `crypto/rand` y almacena `nonce || ciphertext` en los archivos `.bin` del DataNode. Los hashes SHA-256 del texto plano se guardan en `.checksum` para verificar integridad tras descifrado.
* **Control de Acceso y Propiedad (ACL)**:
  * Identidad de usuario transmitida en cabecera gRPC `x-dfs-user` mediante `NewUserTokenCredentials`.
  * Cada archivo y directorio registra su propietario (`owner`), persistido en la FSM de Raft.
  * El directorio raíz `/` es público y permite creación de recursos a cualquier usuario autenticado.
  * Todo usuario tiene acceso total a sus propios archivos y directorios.
  * Usuarios sin autorización reciben `codes.PermissionDenied`.
  * El usuario `admin` posee privilegios de superusuario (acceso irrestricto de lectura y escritura).

---

## 5. Instrucciones de Compilación y Ejecución

### Requisitos
* Go 1.22+
* Docker y Docker Compose

### Compilación y Pruebas Unitarias
Compilar todos los paquetes:
```bash
go build ./...
```

Ejecutar suite completa de pruebas unitarias (incluyendo pruebas de cifrado y ACL):
```bash
go test -v ./...
```

### Despliegue con Docker Compose
Iniciar el clúster con 3 ControlNodes (`cn1`, `cn2`, `cn3`) y 3 DataNodes (`datanode-1`, `datanode-2`, `datanode-3`):
```bash
docker compose up -d --build
```

Verificar estado de los contenedores:
```bash
docker compose ps
```

---

## 6. Guía de Uso del Cliente CLI

El cliente puede ejecutarse de forma interactiva o por comandos individuales:

### Modo Interactivo
```bash
docker compose run --rm client
```
Ejemplo de sesión:
```text
dfsha:/> mkdir docs
dfsha:/> cd docs
dfsha:/docs> pwd
/docs
dfsha:/docs> put /test.bin
dfsha:/docs> ls
dfsha:/docs> cd ..
dfsha:/> ls docs
dfsha:/> exit
```

### Operaciones Directas y Aislamiento Multiusuario
El flag `-user` (o variable `DFS_USER`) define la identidad del cliente:

1. Subir archivo como usuario `alice`:
```bash
docker compose run --rm client -user=alice mkdir /privado_alice
docker compose run --rm client -user=alice put /test.bin /privado_alice
```

2. Intentar acceder como usuario `bob` (bloqueado por control de acceso):
```bash
docker compose run --rm client -user=bob ls /privado_alice
# Error: listing directory /privado_alice: rpc error: code = PermissionDenied desc = permission denied: owned by "alice"
```

3. Acceder con permisos de superusuario `admin`:
```bash
docker compose run --rm client -user=admin ls /privado_alice
```

4. Descargar archivo:
```bash
docker compose run --rm client -user=alice get /privado_alice/test.bin /tmp/test_recuperado.bin
```

5. Eliminar recursos:
```bash
docker compose run --rm client -user=alice rm /privado_alice/test.bin
docker compose run --rm client -user=alice rmdir /privado_alice
```

---

## 7. Validación de Tolerancia a Fallos (Failover del Líder)

1. Detener el contenedor del líder actual (ej. `cn1`):
```bash
docker compose stop cn1
```
2. Ejecutar operaciones desde el cliente sin reiniciar servicios:
```bash
docker compose run --rm --no-deps client ls /
```
3. Los ControlNodes restantes reconfiguran el quórum, eligen un nuevo líder y atienden solicitudes de lectura y escritura manteniendo la consistencia de los datos.
