const dbdatos = require('../conexion');
const etl = {};

iniciar = () =>{
    constantes.tipo = 'tabla';
    constantes.origen = 'seleccione una tabla';
    constantes.destino =  'seleccione una tabla';
    constantes.consulta = '';
    constantes.campos_tabla_olap = [];
    constantes.campos_tabla_oltp = [];
    constantes.campos_seleccionados = []
}

const constantes = {
    tipo: 'tabla',
    origen: 'seleccione una tabla',
    destino: 'seleccione una tabla',
    campos_tabla_olap: [],
    campos_tabla_oltp: [],
    campos_seleccionados: [],
    consulta: '',
}


const lista_etl = [];
let objeto_etl = {
    tabla_destino: '',
    consulta: '',
    campos_a_insertar: [],
}
let mensaje = '_';


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
        constantes.tipo = respuesta.tipo
    };


    //GUARDAR TABLA DESTINO Y ORIGEN O CONSULTA
    if(respuesta.destino != undefined) constantes.destino = respuesta.destino;
    if(constantes.tipo == 'tabla'){
        if(respuesta.origen != undefined) {
            constantes.origen = respuesta.origen;
            constantes.consulta = `select * from ${constantes.origen};`
        };
    }else{
        if(respuesta.consulta != undefined){
            constantes.consulta = respuesta.consulta 
        }
    }




    if(constantes.consulta != ''){/* *************************************************************************************************************************************************** */


                        try {
                            const pool = await dbdatos.getConnection(); //CONECCION A LA BASE DE DATOS
                            //OBTENER LOS CAMPOS EL TIPO DE DATO Y LA LONGITUD DE LA TABLA DE LA BASE DE DATOS DESTINO
                            const campos_tabla_olap = await pool.request().query(`use ${dbdatos.databases.destino}; select column_name, data_type tipo_dato, CHARACTER_MAXIMUM_LENGTH as longitud, 0 as modificar, 'no' as concatenar from information_schema.columns where table_name = '${constantes.destino}'`);     
                            
                            
                            constantes.campos_tabla_olap = campos_tabla_olap.recordset;



                            
                            const campos_tabla_oltp = await pool.request().query(`use ${dbdatos.databases.origen}; ${constantes.consulta.replace('select', 'select top(1) ')}`)
                            if(constantes.campos_tabla_oltp.length === 0){
                                    const campos = campos_tabla_oltp.recordset;
                                    for (let prop in campos[0]){
                                        constantes.campos_tabla_oltp.push(prop);
                                    }
                            }else{

                                if(respuesta.concatenar != undefined) {

                                    

                                    for(let i=0; i<constantes.campos_tabla_olap.length; i++){
                                        constantes.campos_tabla_olap[i].tipo_dato = respuesta.tipo_dato[i];
                                        constantes.campos_tabla_olap[i].longitud = respuesta.longitud[i];
                                        constantes.campos_tabla_olap[i].modificar = respuesta.modificar[i];
                                        constantes.campos_tabla_olap[i].concatenar = respuesta.concatenar[i];
                                        
                                    }
                                }
                            }
                            
                                        
                            
                                    
                                    objeto_etl.tabla_destino = constantes.destino;
                                    objeto_etl.consulta = constantes.consulta;
                                
                            mensaje = '_';
                        } catch (error) {
                            mensaje = error;
                            constantes.destino =  'seleccione una tabla';
                            constantes.campos_tabla_oltp = [];
                        }


    }/* *************************************************************************************************************************************************************************** */

    



/*     console.log('respuesta:')
    console.log(respuesta);*/
    console.log('constantes:')
    console.log(constantes) 


    res.render('etl',{
        dbdatos,
        mensaje,
        constantes
    });
}

module.exports = etl;