FROM python:2.7-alpine3.7

ENV GEAR_BASE_DIR=/flywheel/v0
ENV GEAR_INPUT_DIR="${GEAR_BASE_DIR}/input" \
    GEAR_OUTPUT_DIR="${GEAR_BASE_DIR}/output" \
    GEAR_MANIFEST_FILE="${GEAR_BASE_DIR}/manifest.json" \
    GEAR_ENTRYPOINT="${GEAR_BASE_DIR}/run"

RUN apk add --no-cache git bash build-base python-dev py-pip jpeg-dev zlib-dev \
    && mkdir -p "${GEAR_INPUT_DIR}" \
    && mkdir -p "${GEAR_OUTPUT_DIR}"

# install requirements
COPY requirements.txt "${GEAR_BASE_DIR}/requirements.txt"
RUN pip install -r "${GEAR_BASE_DIR}/requirements.txt"

COPY manifest.json "${GEAR_MANIFEST_FILE}"
COPY run.sh "${GEAR_ENTRYPOINT}"
COPY script.py "${GEAR_BASE_DIR}/script.py"

WORKDIR ${GEAR_BASE_DIR}
