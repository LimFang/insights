import logging


def setup_logging(id):
    gcp_handler = None
    try:
        import google.cloud.logging
        from google.cloud.logging_v2.handlers import CloudLoggingHandler

        client = google.cloud.logging.Client()
        gcp_handler = CloudLoggingHandler(client, labels={"worker_id": id})
    except Exception as e:
        logging.warning(f'GCP logging unavailable ({e}); using console logging only.')

    logging.getLogger().setLevel(logging.INFO)

    if gcp_handler is not None:
        logging.getLogger().addHandler(gcp_handler)

    console_handler = logging.StreamHandler()
    logging.getLogger().addHandler(console_handler)
