const dbdatos = require('../conexion');
const proyectos = {};
const fs = require('fs');
const path = require('path');
/*  ======================================================== CONSTANTES ===============================================================*/
let lista_proyectos = [];
let mensaje = '_';




/*  =============================================== recuperar datos de archivo ===============================================================*/
function obtener_proyectos(){
    var ruta = path.join(__dirname,'..','public', 'proyectos')
    lista_proyectos = fs.readdirSync(ruta);
}


function obtener_proyecto(nombre_proyecto){
    
    try {
        const data = fs.readFileSync(path.join(__dirname, '..', 'public', 'proyectos', nombre_proyecto),'utf8');
        const dbdatoss = JSON.parse(data);
        dbdatos.config_origen = dbdatoss.config_origen;
        dbdatos.config_destino = dbdatoss.config_destino;
        dbdatos.proyecto = dbdatoss.proyecto;
        dbdatos.lista_etl = dbdatoss.lista_etl;
        dbdatos.tablas_origen =  dbdatoss.tablas_origen;
        dbdatos.tablas_destino = dbdatoss.tablas_destino;
        dbdatos.tipo_de_dato = dbdatoss.tipo_de_dato;
        dbdatos.databases = dbdatoss.databases
    } catch (error) {
        console.log(error)
    }
}


  





/*  ======================================================== METODO GET ===============================================================*/
proyectos.get = (req, res) => {
    mensaje = '_'
    obtener_proyectos();
    res.render('proyectos',{
        lista_proyectos,
        mensaje
    });
}








/* ############################################################################################################################################################# */
/*  ==================================================================== METODO POST ===========================================================================*/
/* ############################################################################################################################################################# */
proyectos.post = async (req, res) => {
    mensaje = '_'
    const respuesta = req.body;

    if(respuesta.proyecto != undefined){
        obtener_proyecto(respuesta.proyecto)
        dbdatos.close();
        await dbdatos.getConnection_origen();
        await dbdatos.getConnection_destino();
        res.redirect('/etls')
        return;
    }
    if(respuesta.eliminar_proyecto != undefined){
        try {
            const proyecto_a_eliminar = lista_proyectos[respuesta.eliminar_proyecto];
            fs.unlink(path.join(__dirname, '..', 'public', 'proyectos', proyecto_a_eliminar), async function (err) {
                if (err) throw err;
                lista_proyectos.splice(respuesta.eliminar_proyecto,1)
                mensaje = 'proyecto: *' + proyecto_a_eliminar + '* eliminado correctamente!';
                res.render('proyectos',{
                    lista_proyectos,
                    mensaje
                });
            });
        } catch (error) {
            console.log(error)
        }
    }

}





module.exports = proyectos;

