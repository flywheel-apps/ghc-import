#!/usr/bin/env sh

set -eu
unset CDPATH
cd "$( dirname $0 )/../.."


USAGE="
Usage:
    $0 [OPTION...] [[--] PYTEST_ARGS...]
Runs all tests.
Options:
    -h, --help              Print this help and exit
    -- PYTEST_ARGS          Arguments passed to py.test
"


main() {
    export PYTHONDONTWRITEBYTECODE=1
    export PYTHONPATH=.

    while [ $# -gt 0 ]; do
        case "$1" in
            -h|--help)
                log "$USAGE"
                exit 0
                ;;
            --)
                shift
                break
                ;;
            *)
                break
                ;;
        esac
        shift
    done

    log "INFO: Cleaning pyc and previous coverage results ..."
    find . -type d -name __pycache__ -exec rm -rf {} \; || true
    find . -type f -name '*.pyc' -delete
    rm -rf .coverage htmlcov

    log "INFO: Running tests ..."
    pytest tests/pytest --cov=run --cov-report=

    log "INFO: Reporting coverage ..."
    coverage report --show-missing
}

log() {
    printf "\n%s\n" "$@" >&2
}


main "$@"
