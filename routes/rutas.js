const express = require('express');

const router = express.Router();

const database_conection = require('../controller/database_conection');
const etl = require('../controller/etl');

//para llenar las credenciales para conectarnos a la base de datos
router.get('/', database_conection.get);
router.post('/guardar_credenciales', database_conection.post);

//para configurar el etl
router.get('/etlget', etl.get);
router.post('/etlpost', etl.post);

module.exports = router;