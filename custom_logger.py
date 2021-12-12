import logging

logger = logging.getLogger(__name__)
FORMAT = "[%(asctime)s %(levelname)s%(filename)s:%(lineno)s-%(funcName)s()] %(message)s"
logging.basicConfig(format=FORMAT)
logger.setLevel(logging.DEBUG)