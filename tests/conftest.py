import logging

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)-8s [%(filename)s:%(lineno)d]:%(message)s',
)

logging.getLogger('nebula3').setLevel(logging.DEBUG)
