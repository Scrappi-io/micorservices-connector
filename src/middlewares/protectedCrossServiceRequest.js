module.exports = (config) => {
  return (req, res, next) => {
    if (
      req.headers['cross-service-token'] !== config.crossServiceToken ||
      (req.hostname !== 'localhost' && req.hostname !== '127.0.0.1')
    ) {
      throw new Error('403 Forbidden');
    }

    return next();
  };
};
