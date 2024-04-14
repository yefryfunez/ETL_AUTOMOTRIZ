const dbdatos = require('../conexion');
const etl = {};
const fs = require('fs');
const path = require('path');


function guardar_etl(data){
    fs.writeFile(path.join(__dirname,'datos_etl.txt'), data, (err)=>{
        if(err){
            console.log('Ocurio un error: \n', err)
        }else{
            console.log('ETL guardado exitosamente')
        }
    })
}

function leer_etl(){
    fs.readFile(path.join(__dirname, 'datos_etl.txt'), 'utf8',(err, data)=>{
         if(err) {
            console.log('Ocurrio el siguiente error al leer el archivo: \n', err)
         }
         try {
            const arreg = JSON.parse(data);
            arreg.forEach(elemento => {
                dbdatos.lista_etl.push(elemento)
            })
            
            //console.log(arreg);
         } catch (error) {
            const lista_etl_json = JSON.stringify(dbdatos.lista_etl, null , 2);
            guardar_etl(lista_etl_json);

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
    
    if(dbdatos.lista_etl.length === 0) leer_etl();
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
                            const pool = await dbdatos.getConnection(); //CONEXION A LA BASE DE DATOS
                            
                            //OBTENER LOS CAMPOS EL TIPO DE DATO Y LA LONGITUD DE LA TABLA DE LA BASE DE DATOS DESTINO Y GUARDARLOS EN UN ARREGLO
                            const campos_tabla_olap = await pool.request().query(
                                `use ${dbdatos.databases.destino}; select column_name campo_destino, data_type tipo_dato, CHARACTER_MAXIMUM_LENGTH as longitud, 'Normal' as modificar, 'no' as concatenar from information_schema.columns where table_name = '${constantes.destino.replace('[','').replace(']','')}'`);
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
                                                        if(constantes.campos_tabla_olap[i].concatenar === 'si'){
                                                            concat -= 1;
                                                            constantes.campos_tabla_olap[i].campo_origen = respuesta.campos_a_concatenar;
                                                            if(Array.isArray(respuesta.campos_a_concatenar))
                                                                campos += 'concat(' + constantes.campos_tabla_olap[i].campo_origen.join(' ,\'  \', ') + ') as ' + constantes.campos_tabla_olap[i].campo_destino + ', ';
                                                        } else {
                                                            if(Array.isArray(respuesta.campo_origen)) constantes.campos_tabla_olap[i].campo_origen = respuesta.campo_origen[concat]; 
                                                            else constantes.campos_tabla_olap[i].campo_origen = respuesta.campo_origen; 

                                                            if(respuesta.campo_origen[concat] != 'ninguno')
                                                                campos += respuesta.campo_origen[concat] + ', '; 
                                                        }
                                                    }
                                                    concat += 1;
                                                }
                                                 campos = campos.slice(0,campos.length-2)
                                                if(campos != '')
                                                    constantes.consulta = `select ${campos} from ${constantes.origen};`
                                        }
                            }
                            
                            

                            /* GUARDAR LOS DATOS DE MODIFICAR O CONCATENAR ASI COMO EL TIPO DE DATOS Y EL CAMPO ORIGEN PAR CADA CAMPO DESTINO, EN CASO DE QUE SE ELIJA CONSULTA*/
                            if(constantes.tipo === 'consulta'){
                                if(constantes.consulta != '' && respuesta.campo_origen != undefined){

                                    for(let i=0; i<constantes.campos_tabla_olap.length; i++){
                                                        constantes.campos_tabla_olap[i].tipo_dato = respuesta.tipo_dato[i];
                                                        constantes.campos_tabla_olap[i].longitud = respuesta.longitud[i];
                                                        constantes.campos_tabla_olap[i].modificar = respuesta.modificar[i];
                                                        constantes.campos_tabla_olap[i].campo_origen = respuesta.campo_origen[i];
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
                                    const lista_etl_json = JSON.stringify(dbdatos.lista_etl, null , 2);
                                    guardar_etl(lista_etl_json)
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



   
    res.render('guardar_etl',{
        dbdatos,
        mensaje,
        constantes
    });
}





module.exports = etl;