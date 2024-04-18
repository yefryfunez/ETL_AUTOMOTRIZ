let dbdatos = require('../conexion');
const etl = {};
const fs = require('fs');
const path = require('path');


function guardar_etl(data){
    fs.writeFile(path.join(__dirname, '..', 'public', 'proyectos', dbdatos.proyecto+'.txt'), data, (err)=>{
        if(err){
            console.log('Ocurio un error: \n', err)
        }else{
            console.log('ETL guardado exitosamente')
        }
    })
}

function leer_datos(){
    fs.readFile(path.join(__dirname, '..', 'public', 'proyectos', dbdatos.proyecto+'.txt'), 'utf8',(err, data)=>{
         if(err) {
            console.log('Ocurrio el siguiente error al leer el archivo: \n', err)
         }
         try {
            const dbdatoss = JSON.parse(data);
            dbdatos.config = dbdatoss.config;
            dbdatos.proyecto = dbdatoss.proyecto;
            dbdatos.lista_etl = dbdatoss.lista_etl;
            dbdatos.tablas_origen =  dbdatoss.tablas_origen;
            dbdatos.tablas_destino = dbdatoss.tablas_destino;
            dbdatos.tipo_de_dato = dbdatoss.tipo_de_dato;
            dbdatos.databases = dbdatoss.databases
         } catch (error) {
            data = JSON.stringify(dbdatos, null , 2);
            guardar_etl(data);
         }
    })
}


/*  ======================================================== CONSTANTES ===============================================================*/
const constantes = {
    tipo: 'tabla',
    origen: 'seleccione una tabla',
    destino: 'seleccione una tabla',
    campos_tabla_olap: [],
    campos_tabla_oltp: [],
    consulta: '',
}
let mensaje = '_';

 


function  iniciar () {
    constantes.tipo = 'tabla';
    constantes.origen = 'seleccione una tabla';
    constantes.destino =  'seleccione una tabla';
    constantes.campos_tabla_olap = [];
    constantes.campos_tabla_oltp = [];
    constantes.consulta = '';
    mensaje = '_'
}












/*  ======================================================== METODO GET ===============================================================*/
etl.get = (req, res) => {
    console.log('respuesta obtenida. ',dbdatos)
    if(dbdatos.lista_etl.length === 0) leer_datos();
    iniciar();
    res.render('guardar_etl',{
        dbdatos,
        mensaje,
        constantes
    });
}












/* ############################################################################################################################################################# */
/*  ==================================================================== METODO POST ===========================================================================*/
/* ############################################################################################################################################################# */
etl.post = async (req, res) => {
    const respuesta = req.body;


    //UNA VEZ SELECCIONADA LA TABLA O CONSULTA PARA OBTENER LOS DATOS DE LA BASE DE DATOS ORIGEN
    /* *************************************************************************************************************************************************** */
    if(respuesta.tipo != undefined) {
        if(constantes.tipo != respuesta.tipo) iniciar();
        constantes.tipo = respuesta.tipo;
    };
    console.log(respuesta)


    //GUARDAR TABLA ORIGEN O CONSULTA
    /* *************************************************************************************************************************************************** */
    if(constantes.tipo == 'tabla'){
        if(respuesta.origen != undefined) {
                        constantes.origen = '['+respuesta.origen+']';
                        constantes.consulta = `select * from ${constantes.origen};`
                        constantes.campos_tabla_oltp = [];
        };
    }else{
        if(respuesta.consulta != undefined){
                        constantes.consulta = respuesta.consulta 
                        constantes.campos_tabla_oltp = [];
        }
    }




    //GUARDAR TABLA DESTINO
    /* *************************************************************************************************************************************************** */
    if(respuesta.destino != undefined)  {
            constantes.destino = '[' + respuesta.destino + ']' 
            
            //validacion para verificar que no exista un etl para la tabla destino seleccionada
            dbdatos.lista_etl.forEach(elemento => {
                        if(elemento.destino === constantes.destino){
                                constantes.consulta = '';
                                constantes.origen = 'seleccione una tabla';
                                mensaje = 'Ya existe un etl para la dimension => '+respuesta.destino;
                                return;
                        }
            })
    }
    






    if(constantes.consulta != '' ){/* *************************************************************************************************************************************************** */


                    try {
                            dbdatos.close();
                            const pool_destino = await dbdatos.getConnection_destino(); //CONEXION A LA BASE DE DATOS DESTINO
                            const pool_origen = await dbdatos.getConnection_origen();   //CONEXION A LA BASE DE DATOS ORIGEN
                            
                            //OBTENER LOS CAMPOS EL TIPO DE DATO Y LA LONGITUD DE LA TABLA DE LA BASE DE DATOS DESTINO Y GUARDARLOS EN UN ARREGLO
                            const campos_tabla_olap = await pool_destino.request().query(
                                `use ${dbdatos.databases.destino}; select column_name campo_destino, data_type tipo_dato, CHARACTER_MAXIMUM_LENGTH as longitud, CHARACTER_MAXIMUM_LENGTH as longitud_maxima, 'Normal' as modificar, 'no' as concatenar from information_schema.columns where table_name = '${constantes.destino.replace('[','').replace(']','')}'`);
                            constantes.campos_tabla_olap = campos_tabla_olap.recordset;
                            
                            
                            //OBTENER LOS CAMPOS DE LA BASE DE DATOS ORIGEN SELECCIONADA O DE LA CONSULTA
                            const campos_tabla_oltp = await pool_origen.request().query(`use ${dbdatos.databases.origen}; ${constantes.consulta.replace('select', 'select top(1) ')}`)

                            if(constantes.campos_tabla_oltp.length === 0 ){
                                        const campos = campos_tabla_oltp.recordset;
                                        constantes.campos_tabla_oltp.push('ninguno')
                                        for (let prop in campos[0]){
                                            constantes.campos_tabla_oltp.push(prop);
                                        }
                            }else{

                                                        /* GUARDAR LOS DATOS DE MODIFICAR O CONCATENAR ASI COMO EL TIPO DE DATOS Y EL CAMPO ORIGEN PAR CADA CAMPO DESTINO, CUANDO SE SELECCIONA TABLA */
                                                        if(respuesta.concatenar != undefined) {
                                                            let concat = 0;
                                                            let campos = '';
                                                            for(let i=0; i<constantes.campos_tabla_olap.length; i++){
                                                                constantes.campos_tabla_olap[i].tipo_dato = respuesta.tipo_dato[i];
                                                                constantes.campos_tabla_olap[i].longitud = respuesta.longitud[i];
                                                                constantes.campos_tabla_olap[i].modificar = respuesta.modificar[i];
                                                                constantes.campos_tabla_olap[i].concatenar = respuesta.concatenar[i];

                                                                if(respuesta.campo_origen != undefined) {
                                                                    if(constantes.campos_tabla_olap[i].concatenar === 'si' && respuesta.campos_a_concatenar != undefined){
                                                                        concat -= 1;
                                                                        constantes.campos_tabla_olap[i].campo_origen = respuesta.campos_a_concatenar;
                                                                        if(Array.isArray(respuesta.campos_a_concatenar))    campos += constantes.campos_tabla_olap[i].campo_origen.join(', ') + ', ';
                                                                        else    campos += constantes.campos_tabla_olap[i].campo_origen + ', ';
                                                                    } else {
                                                                        if(Array.isArray(respuesta.campo_origen)) constantes.campos_tabla_olap[i].campo_origen = respuesta.campo_origen[concat]; 
                                                                        else constantes.campos_tabla_olap[i].campo_origen = respuesta.campo_origen; 
        
                                                                        if(constantes.campos_tabla_olap[i].campo_origen != 'ninguno')
                                                                            campos += constantes.campos_tabla_olap[i].campo_origen + ', '; 
                                                                    }
                                                                }
                                                                concat += 1;
                                                            }
                                                            //eliminar campos duplicados
                                                            campos = campos.slice(0,campos.length-2)
                                                            let palabras = campos.split(', ');
                                                            let palabrasUnicas = [...new Set(palabras)];
                                                            campos = palabrasUnicas.join(', ');

                                                            // console.log('**********************************campos*************************************************')
                                                            // console.log(campos);
                                                            if(campos != '')
                                                                constantes.consulta = `select ${campos} from ${constantes.origen};`
                                                                console.log(constantes.consulta)
                                                        }else{
                                                            if(constantes.consulta != '' && respuesta.campo_origen != undefined){

                                                                for(let i=0; i<constantes.campos_tabla_olap.length; i++){
                                                                                    constantes.campos_tabla_olap[i].tipo_dato = respuesta.tipo_dato[i];
                                                                                    constantes.campos_tabla_olap[i].longitud = respuesta.longitud[i];
                                                                                    constantes.campos_tabla_olap[i].modificar = respuesta.modificar[i];
                                                                                    constantes.campos_tabla_olap[i].campo_origen = respuesta.campo_origen[i];
                                                                }
                                                                //console.log(constantes.campos_tabla_olap);
                                                            }
                                                        }
                            }
             
                            


                            


                            /* =========================== GUARDAR EL ETL PARA LA DIMENSION ESPECIFICADA ============================== */
                            if(respuesta.guardar != undefined ){
                                if(respuesta.guardar === 'guardar'){

                                    dbdatos.lista_etl.push(
                                        {
                                            tipo: constantes.tipo,
                                            origen: constantes.origen,
                                            destino: constantes.destino,
                                            campos_tabla_olap: constantes.campos_tabla_olap,
                                            campos_tabla_oltp: constantes.campos_tabla_oltp,
                                            consulta: constantes.consulta,
                                        }

                                    )
                                    const datos = JSON.stringify(dbdatos, null , 2);
                                    guardar_etl(datos)
                                    
                                }

                                iniciar();
                                res.redirect('/etls');
                                return;
                            }

                
                            mensaje = '_';
                    } catch (error) {
                            mensaje = error;
                            constantes.destino =  'seleccione una tabla';
                            constantes.campos_tabla_oltp = [];
                    }


    }/* *************************************************************************************************************************************************************************** */



   
    res.render('guardar_etl',{
        dbdatos,
        mensaje,
        constantes
    });
}





module.exports = etl;