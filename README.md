# Video Service API

Este microservicio gestiona la generación asíncrona de videos promocionales mediante el modelo de Google Vertex AI (`veo-3.1-fast-generate-001`).

Debido a las estrictas regulaciones y requisitos de hardware del motor **Veo 3.1**, la API controla matemáticamente la duración de los videos, ignorando cualquier input de duración externa.

## Reglas y Permisos Físicos de Veo 3.1
- **Videos Base (Generación):** Siempre tendrán una duración forzada de **8 segundos**.
- **Videos Extendidos (Continuación):** Siempre tendrán una duración forzada de **7 segundos**.
- Las imágenes proporcionadas inicialmente deben empatar con el `aspect_ratio` físico (ej. foto 9:16 = video 9:16).
- Los videos se vuelcan *automáticamente* de Vertex AI a nuestro Cloud Storage `pneuma_bucket` evitando el uso intensivo de RAM del servidor en la mayoría de los casos.

---

## 1. Generar Video (Base)
Inicia un Image-to-Video generation job creando el anuncio principal de 8 segundos.

* **Endpoint:** `POST /api/v1/video/generate`
* **Content-Type:** `multipart/form-data`

### Input (Request Form)
1. **`images`** *(Lista de Archivos)*: Lista de imágenes UploadFile. Se admite un **máximo de 3 imágenes**, caso contrario devuelve Error 400 (para protección anti-spam). _Nota: En el backend actual, Cloud Vertex AI toma la primera foto del array como "Image Seed" inicial._
2. **`prompt_veo_visual`** *(String, Requerido)*: Las instrucciones de cámara y visuales en inglés.
3. **`prompt_veo_audio`** *(String, Opcional)*: Instrucciones del género o fondo de audio. Si se provee, activa automáticamente el parámetro interno `generateAudio: true`.
4. **`aspect_ratio`** *(String, Defecto: "16:9")*: La orientación final ("16:9", "9:16", "1:1").

### Output (Response HTTP 202)
El request desencadena una operación de predicción larga en la nube y devuelve el UUID de seguimiento local:
```json
{
  "video_id": "c3fc1849-6a58-407b-bc54-a1dc407a34f7",
  "status": "PROCESSING"
}
```

---

## 2. Extender Video (Continuación)
Inicia un Video-to-Video generation job de **7 segundos**, usando como base física (gcsUri) el último segundo lógico de un video previamente completado en la plataforma.

* **Endpoint:** `POST /api/v1/video/extend`
* **Content-Type:** `application/json`

### Input (JSON Body)
```json
{
  "video_id": "c3fc1849-6a58-407b-bc54-a1dc407a34f7",
  "prompt_veo_visual": "Camera follows the running character into the sunset.",
  "prompt_veo_audio": "Epic rock music finale."
}
```
*Atención:* El backend automáticamente lee el ticket antiguo Firestore de ese `video_id`, hereda obligatoriamente el `aspect_ratio` del video original, y lanza una extensión forzada a `7` segundos.

### Output (Response HTTP 202)
Se generará un *Nuevo UUID* `new_video_id` para la continuación resultante, preservando el viejo sin tocar.
```json
{
  "video_id": "f1ffafbb-3cf3-4064-9955-2582d71e6001",
  "status": "PROCESSING"
}
```

---

## 3. Consultar Estado (Polling)
Como Vertex AI puede demorar minutos en modelar 8 segundos de video en calidad alta, el cliente debe consultar periódicamente (polling cada 5s-10s) el estado del Job hasta que se resuelva la operación LRO (Long Running Operation).

* **Endpoint:** `GET /api/v1/video/status/{video_id}`

### Outputs Intermedios (Response HTTP 200)

**Cuando sigue procesando (PROCESSING):**
```json
{
  "video_id": "c3fc1849-6a58-4...",
  "status": "PROCESSING",
  "progress": {
     "predictLongRunningMetadata": {}
  }
}
```

**Cuando finaliza con éxito (COMPLETED):**
Devuelve la URL firmada provisoria autorizada en Google Cloud Storage listísima para mostrarse en el HTML Video Player. Automáticamente se guarda en la DB Firestore.
```json
{
  "video_id": "c3fc1849-6a58-4...",
  "status": "COMPLETED",
  "video_url": "https://storage.googleapis.com/pneuma_bucket/...&GoogleAccessId=..."
}
```

**Cuando falla por alguna restricción AI o cuotas (FAILED):**
```json
{
  "video_id": "c3fc1849-6a58-4...",
  "status": "FAILED",
  "error": "The request aspect ratio ASPECT_RATIO_16_9 doesn't match the width 1080 and height 1920."
}
```
