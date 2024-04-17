let dbdatos = require('../conexion');
const actualizar_etl = {};
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
    i: -1,
}
let mensaje = '_';

 


function  iniciar () {
    constantes.tipo = 'tabla';
    constantes.origen = 'seleccione una tabla';
    constantes.destino =  'seleccione una tabla';
    constantes.campos_tabla_olap = [];
    constantes.campos_tabla_oltp = [];
    constantes.consulta = '';
    constantes.i = -1;
}












/*  ======================================================== METODO GET ===============================================================*/
actualizar_etl.get = (req, res) => {
    if(dbdatos.lista_etl.length === 0) leer_datos();
    iniciar();
    res.render('actualizar_etl',{
        dbdatos,
        mensaje,
        constantes
    });
}






function buscarETL(destino){
    for(let i=0; i<dbdatos.lista_etl.length; i++){
        if(dbdatos.lista_etl[i].destino === destino) return i;
    }
    return -1;
}





/* ############################################################################################################################################################# */
/*  ==================================================================== METODO POST ===========================================================================*/
/* ############################################################################################################################################################# */
actualizar_etl.post = async (req, res) => {
    const respuesta = req.body;
console.log(respuesta)


    if(respuesta.Eliminar != undefined){
        if(respuesta.Eliminar === 'Eliminar'){
            dbdatos.lista_etl.splice(0,dbdatos.lista_etl.length);
            const lista_etl_json = JSON.stringify(dbdatos, null , 2);
            guardar_etl(lista_etl_json)
            
        }
    } 
    

    // ELIMINA EL ETL AL QUE SE LE HA DADO CLICK
    /* *************************************************************************************************************************************************** */
    if(respuesta.eliminar_etl != undefined){
        dbdatos.lista_etl.splice(respuesta.eliminar_etl,1);
        const lista_etl_json = JSON.stringify(dbdatos, null , 2);
        guardar_etl(lista_etl_json)
    } 
        // MUEVE HACIA ARRIBA EL ETL AL QUE SE LE HA DADO CLICK
    /* *************************************************************************************************************************************************** */
    if(respuesta.subir_etl != undefined){
        [dbdatos.lista_etl[parseInt(respuesta.subir_etl)-1], dbdatos.lista_etl[parseInt(respuesta.subir_etl)]] = [dbdatos.lista_etl[parseInt(respuesta.subir_etl)], dbdatos.lista_etl[parseInt(respuesta.subir_etl)-1]];
        const lista_etl_json = JSON.stringify(dbdatos, null , 2);
        guardar_etl(lista_etl_json)
        iniciar()
    } 
        // MUEVE HACIA ABAJO EL ETL AL QUE SE LE HA DADO CLICK
    /* *************************************************************************************************************************************************** */
    if(respuesta.bajar_etl != undefined){
        [dbdatos.lista_etl[parseInt(respuesta.bajar_etl)], dbdatos.lista_etl[parseInt(respuesta.bajar_etl)+1]] = [dbdatos.lista_etl[parseInt(respuesta.bajar_etl)+1], dbdatos.lista_etl[parseInt(respuesta.bajar_etl)]];
        const lista_etl_json = JSON.stringify(dbdatos, null , 2);
        guardar_etl(lista_etl_json)
        iniciar()
    } 
    

    // MUESTRA LOS DATOS DEL ETL AL QUE SE LE HA DADO CLICK
    /* *************************************************************************************************************************************************** */
    if(respuesta.etl != undefined){
        constantes.i = buscarETL(respuesta.etl);
        constantes.tipo = dbdatos.lista_etl[constantes.i].tipo;
        constantes.origen = dbdatos.lista_etl[constantes.i].origen;
        constantes.destino = dbdatos.lista_etl[constantes.i].destino;
        constantes.campos_tabla_olap = dbdatos.lista_etl[constantes.i].campos_tabla_olap;
        constantes.campos_tabla_oltp = dbdatos.lista_etl[constantes.i].campos_tabla_oltp;
        constantes.consulta = dbdatos.lista_etl[constantes.i].consulta;
    }



















    //UNA VEZ SELECCIONADA LA TABLA O CONSULTA PARA OBTENER LOS DATOS DE LA BASE DE DATOS ORIGEN
    /* *************************************************************************************************************************************************** */
    if(respuesta.tipo != undefined) {
        var i = constantes.i;
        iniciar();
        constantes.i = i;
        constantes.tipo = respuesta.tipo;
    };



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
                        constantes.consulta = respuesta.consulta ;
                        constantes.campos_tabla_oltp = [];
        }
    }




    //GUARDAR TABLA DESTINO
    /* *************************************************************************************************************************************************** */
    if(respuesta.destino != undefined)  {
            constantes.destino = '[' + respuesta.destino + ']' 
            const it = buscarETL(respuesta.destino);
            if(it != -1){
                dbdatos.lista_etl.splice(it,1);
            }
    }
    






    if(constantes.consulta != '' ){/* *************************************************************************************************************************************************** */


                        try {
            if(respuesta.etl === undefined){
                            const pool = await dbdatos.getConnection(); //CONEXION A LA BASE DE DATOS
                            
                            //OBTENER LOS CAMPOS EL TIPO DE DATO Y LA LONGITUD DE LA TABLA DE LA BASE DE DATOS DESTINO Y GUARDARLOS EN UN ARREGLO
                            const campos_tabla_olap = await pool.request().query(
                                `use ${dbdatos.databases.destino}; select column_name campo_destino, data_type tipo_dato, CHARACTER_MAXIMUM_LENGTH as longitud, CHARACTER_MAXIMUM_LENGTH as longitud_maxima, 'Normal' as modificar, 'no' as concatenar from information_schema.columns where table_name = '${constantes.destino.replace('[','').replace(']','')}'`);
                            constantes.campos_tabla_olap = campos_tabla_olap.recordset;
                            
                            
                            //OBTENER LOS CAMPOS DE LA BASE DE DATOS ORIGEN SELECCIONADA O DE LA CONSULTA
                            const campos_tabla_oltp = await pool.request().query(`use ${dbdatos.databases.origen}; ${constantes.consulta.replace('select', 'select top(1) ')}`)
                            if(constantes.campos_tabla_oltp.length === 0){
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
                            

            }


                            /* =========================== GUARDAR EL ETL PARA LA DIMENSION ESPECIFICADA ============================== */
                            if(respuesta.actualizar != undefined ){
                                if(respuesta.actualizar === 'actualizar'){

                                    dbdatos.lista_etl[constantes.i].tipo = constantes.tipo;
                                    dbdatos.lista_etl[constantes.i].origen =  constantes.origen;
                                    dbdatos.lista_etl[constantes.i].destino = constantes.destino;
                                    dbdatos.lista_etl[constantes.i].campos_tabla_olap = constantes.campos_tabla_olap;
                                    dbdatos.lista_etl[constantes.i].campos_tabla_oltp = constantes.campos_tabla_oltp;
                                    dbdatos.lista_etl[constantes.i].consulta = constantes.consulta;

                                    const datos = JSON.stringify(dbdatos, null , 2);
                                    guardar_etl(datos)
                                }

                                iniciar();
                            }

                
                            mensaje = '_';
                        } catch (error) {
                            mensaje = error;
                            constantes.destino =  'seleccione una tabla';
                            constantes.campos_tabla_oltp = [];
                        }


    }/* *************************************************************************************************************************************************************************** */



   
    res.render('actualizar_etl',{
        dbdatos,
        mensaje,
        constantes
    });
}





module.exports = actualizar_etl;