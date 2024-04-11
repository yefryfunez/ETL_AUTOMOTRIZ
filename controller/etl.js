const dbdatos = require('../conexion');
const etl = {};

iniciar = () =>{
    constantes.tipo = 'tabla';
    constantes.origen = 'seleccione una tabla';
    constantes.destino =  'seleccione una tabla';
    constantes.consulta = '';
    constantes.campos_tabla_olap = [];
    constantes.campos_tabla_oltp = [];
    constantes.campos_seleccionados = [];
    constantes.consulta_final = '';
    constantes.campos_a_concatenar = [];
}

const constantes = {
    tipo: 'tabla',
    origen: 'seleccione una tabla',
    destino: 'seleccione una tabla',
    campos_tabla_olap: [],
    campos_tabla_oltp: [],
    campos_seleccionados: [],
    consulta: '',
    consulta_final: '',
    campos_a_concatenar: []
}


const lista_etl = [];
let objeto_etl = {
    tabla_destino: '',
    consulta: '',
    campos_a_insertar: [],
}
let mensaje = '_';





function concatenarSqlServer(arreglo, modificar,alias){
    let campos = '';
    campos = arreglo.join(',\'  \',');
    campos = 'concat('+campos+')';
    if(modificar === 'Mayuscula') campos = 'upper('+campos+')';
    if(modificar === 'Minuscula') campos = 'lower('+campos+')';
    campos = campos + ' as ' + alias;
    return campos;
  }









etl.get = (req, res) => {
    iniciar();
    res.render('etl',{
        dbdatos,
        mensaje,
        constantes
    });
}





etl.post = async (req, res) => {
    const respuesta = req.body;


    //UNA VEZ SELECCIONADA LA TABLA O CONSULTA PARA OBTENER LOS DATOS DE LA BASE DE DATOS ORIGEN
    /* *************************************************************************************************************************************************** */
    if(respuesta.tipo != undefined) {

        if(constantes.tipo != respuesta.tipo){
            iniciar();
        }
        constantes.tipo = respuesta.tipo;
    };


    //GUARDAR TABLA DESTINO Y ORIGEN O CONSULTA
    if(respuesta.destino != undefined) constantes.destino = respuesta.destino;
    

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


    if(constantes.consulta != ''){/* *************************************************************************************************************************************************** */


                        try {
                            const pool = await dbdatos.getConnection(); //CONEXION A LA BASE DE DATOS
                            //OBTENER LOS CAMPOS EL TIPO DE DATO Y LA LONGITUD DE LA TABLA DE LA BASE DE DATOS DESTINO
                            const campos_tabla_olap = await pool.request().query(
                                `use ${dbdatos.databases.destino}; select column_name  campo_destino, data_type tipo_dato, CHARACTER_MAXIMUM_LENGTH as longitud, 'Normal' as modificar, 'no' as concatenar from information_schema.columns where table_name = '${constantes.destino}'`);     
                            
                            constantes.campos_tabla_olap = campos_tabla_olap.recordset;


                            
                            const campos_tabla_oltp = await pool.request().query(`use ${dbdatos.databases.origen}; ${constantes.consulta.replace('select', 'select top(1) ')}`)
                            if(constantes.campos_tabla_oltp.length === 0){
                                        const campos = campos_tabla_oltp.recordset;
                                        for (let prop in campos[0]){
                                            constantes.campos_tabla_oltp.push(prop);
                                        }
                            }else{

                                        if(respuesta.concatenar != undefined) {
                                                let concat = 0;
                                                for(let i=0; i<constantes.campos_tabla_olap.length; i++){
                                                    constantes.campos_tabla_olap[i].tipo_dato = respuesta.tipo_dato[i];
                                                    constantes.campos_tabla_olap[i].longitud = respuesta.longitud[i];
                                                    constantes.campos_tabla_olap[i].modificar = respuesta.modificar[i];
                                                    constantes.campos_tabla_olap[i].concatenar = respuesta.concatenar[i];

                                                    if(respuesta.campo_origen != undefined) {
                                                        if(constantes.campos_tabla_olap[i].concatenar === 'si'){
                                                            concat -= 1;
                                                            constantes.campos_tabla_olap[i].campo_origen = respuesta.campos_a_concatenar;
                                                        } else {
                                                            constantes.campos_tabla_olap[i].campo_origen = respuesta.campo_origen[concat]; 
                                                        }
                                                        console.log('*************************************campos origen: ' , constantes.campos_tabla_olap[i].campo_origen)
                                                    }
                                                    concat += 1;
                                                }
                                        }
                            }
                            
                                        
                            
                                
                            mensaje = '_';
                        } catch (error) {
                            mensaje = error;
                            constantes.destino =  'seleccione una tabla';
                            constantes.campos_tabla_oltp = [];
                        }


    }/* *************************************************************************************************************************************************************************** */

    //GUARDAR LOS CAMPOS DE LA TABLA ORIGEN SELECCIONADOS
/*     if(respuesta.campo_origen != undefined) {
                constantes.campos_seleccionados = respuesta.campo_origen;
                for(let i = 0; i<constantes.campos_tabla_olap.length; i++){ç
                    if(constantes.campos_tabla_olap[i].modificar === 'Mayuscula') {
                        constantes.campos_seleccionados[i] = 'upper('+ constantes.campos_seleccionados[i] + ') as ' + constantes.campos_tabla_olap[i].column_name;
                    } else if(constantes.campos_tabla_olap[i].modificar === 'Minuscula'){
                        constantes.campos_seleccionados[i] = 'lower('+ constantes.campos_seleccionados[i] + ') as ' + constantes.campos_tabla_olap[i].column_name;
                    } else {
                        constantes.campos_seleccionados[i] =  constantes.campos_seleccionados[i] + ' as ' + constantes.campos_tabla_olap[i].column_name;
                    }
                }
    } */






    console.log('\n.')
    console.log('\n.')
    console.log('\n.')
    console.log('\n.')
    console.log('\n.')
    console.log('\n.')
    console.log('\n.')
    console.log('\n.')
    console.log('\n.')
    console.log('\n.')
    console.log(dbdatos.config)
     console.log('respuesta:')
    console.log(respuesta);
    console.log('constantes:')
    console.log(constantes) 

    res.render('etl',{
        dbdatos,
        mensaje,
        constantes
    });
}


etl.guardarETL = (req, res) => {
    constantes.campos_seleccionados = req.campo_origen;
    res.render('etl',{
        dbdatos,
        mensaje,
        constantes
    });
}

module.exports = etl;