module.exports = (config) => {
  return (req, res, next) => {
    if (req.headers['cross-service-token'] !== config.crossServiceToken || req.hostname !== 'localhost') {
      throw new Error('403 Forbidden');
    }

    return next();
  };
};
