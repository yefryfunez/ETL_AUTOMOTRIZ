const dbdatos = require('../conexion');
const ejecutar_etl = {};
const fs = require('fs');
const path = require('path');
const sql = require('mssql');
/*  ======================================================== CONSTANTES ===============================================================*/

let mensaje = '_';
let filas_afectadas = 0;
let etl_ejecutados = []



/*  =============================================== recuperar datos de archivo ===============================================================*/

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

            //console.log(dbdatos.lista_etl);
         } catch (error) {
            const lista_etl_json = JSON.stringify(dbdatos.lista_etl, null , 2);
            guardar_etl(lista_etl_json);
         }
    })
}



 





/*  ======================================================== METODO GET ===============================================================*/
ejecutar_etl.get = (req, res) => {
    if(dbdatos.lista_etl.length === 0) leer_etl();
    etl_ejecutados = [];
    res.redirect('ejecutar_etl_p')
}








/* ############################################################################################################################################################# */
/*  ==================================================================== METODO POST ===========================================================================*/
/* ############################################################################################################################################################# */
ejecutar_etl.post = async (req, res) => {
    
    if(dbdatos.lista_etl.length >= 0){//1IF------------------------------------------------------------------------------------------------------------------------------------------------------- 
        
        try {
                    const pool = await dbdatos.getConnection();
                    
                    
                    /* ************************************************************** EXTRACCION ***************************************************************** */
                    console.log('destino: ',dbdatos.databases.destino);
                    let tablas_a_limpiar = '';
                    for(let i=dbdatos.lista_etl.length-1; i>=0; i--){
                        tablas_a_limpiar += ` delete from ${dbdatos.lista_etl[i].destino}; `
                    }
                    
                    
                    await pool.request().query(`use ${dbdatos.databases.destino}; ${tablas_a_limpiar}`)
                    
                    for(let elemento of dbdatos.lista_etl){
                           filas_afectadas = 0; 
                           mensaje = '_';
                            let datos = await pool.request().query(`use ${dbdatos.databases.origen}; ${elemento.consulta}`);//obtener los datos de la tabla origen especificada en el etl
                            datos = datos.recordset;


                        












                    /* *********************************************************** TRANSFORMACION ***************************************************************** */

                        let datos_modificados = [];
                        datos.forEach(registro =>{
                            let registro_modificado = {};
                            elemento.campos_tabla_olap.forEach(info => {
                                if(info.campo_origen != 'ninguno'){
                                    if(info.concatenar === 'si'){
                                            if(Array.isArray(info.campo_origen)){
                                                    let concatenacion = '';
                                                    info.campo_origen.forEach(e => {
                                                        concatenacion += registro[e].trim() + ' ';
                                                    })
                                                registro_modificado[info.campo_destino] = concatenacion.substring(0,info.longitud).trim();
                                            } else {
                                                registro_modificado[info.campo_destino] = registro[info.campo_origen].substring(0,info.longitud).trim();
                                            }
                                    }else{
                                        if(typeof registro[info.campo_origen] === 'string')
                                            registro_modificado[info.campo_destino] = registro[info.campo_origen].substring(0,info.longitud).trim();
                                        else
                                            registro_modificado[info.campo_destino] = registro[info.campo_origen];
                                        
                                    }
                                    if(info.modificar === 'Mayuscula') registro_modificado[info.campo_destino] = registro_modificado[info.campo_destino].toUpperCase();
                                    if(info.modificar === 'Minuscula') registro_modificado[info.campo_destino] = registro_modificado[info.campo_destino].toLowerCase();
                                }
                            })
                            datos_modificados.push(registro_modificado);
                        })
                        //console.log(datos_modificados)







                            /* ASIGNACION DE NOMRBE DE CAMPOS Y VARIABLES PAR CARGAR LOS DATOS */
                            let columnas = '';
                            let alias = '';
                            elemento.campos_tabla_olap.forEach( campo => {
                                if(campo.campo_origen != 'ninguno') {
                                    columnas += campo.campo_destino + ', '
                                    alias += '@' + campo.campo_destino + ', '
                                }
                            })
                            columnas = columnas.substring(0,columnas.length-2)
                            alias = alias.substring(0,alias.length-2)

                            







                            
                            /* ************************************************************** CARGA ***************************************************************** */

                                let tipo_dato = '';
                                for(let objeto of datos_modificados){
                                    let icampos = 0;
                                    const request = pool.request();
                                            for(let prop in objeto){
                                                try{
                                                        if(elemento.campos_tabla_olap[icampos].campo_origen === 'ninguno') icampos++;
                                                        data = '';
                                                        tipo_dato = '';
                                                        switch(elemento.campos_tabla_olap[icampos].tipo_dato){
                                                                case 'bigint': tipo_dato = sql.BigInt; break;
                                                                case 'binary': tipo_dato = sql.Binary; break;
                                                                case 'bit': tipo_dato = sql.Bit; break;
                                                                case 'char': tipo_dato = sql.Char(parseInt(elemento.campos_tabla_olap[icampos].longitud)); break;
                                                                case 'date': tipo_dato = sql.Date; break;
                                                                case 'datetime': tipo_dato = sql.DateTime; break;
                                                                case 'datetime2': tipo_dato = sql.DateTime2; break;
                                                                case 'datetimeoffset': tipo_dato = sql.DateTimeOffset; break;
                                                                case 'decimal': tipo_dato = sql.Decimal; break;
                                                                case 'float': tipo_dato = sql.Float; break;
                                                                case 'geography': tipo_dato = sql.Geography; break;
                                                                case 'geometry': tipo_dato = sql.Geometry; break;
                                                                case 'image': tipo_dato = sql.Image; break;
                                                                case 'int': tipo_dato = sql.Int; break;
                                                                case 'money': tipo_dato = sql.Money; break;
                                                                case 'nchar': tipo_dato = sql.NChar(parseInt(elemento.campos_tabla_olap[icampos].longitud)); break;
                                                                case 'ntext': tipo_dato = sql.NText; break;
                                                                case 'numeric': tipo_dato = sql.Numeric; break;
                                                                case 'nvarchar': tipo_dato = sql.NVarChar(parseInt(elemento.campos_tabla_olap[icampos].longitud)); break;
                                                                case 'real': tipo_dato = sql.Real; break;
                                                                case 'smalldatetime': tipo_dato = sql.SmallDateTime; break;
                                                                case 'smallint': tipo_dato = sql.SmallInt; break;
                                                                case 'smallmoney': tipo_dato = sql.SmallMoney; break;
                                                                case 'tipo_dato = sql_variant': tipo_dato = sql.Variant; break;
                                                                case 'text': tipo_dato = sql.Text; break;
                                                                case 'time': tipo_dato = sql.Time; break;
                                                                case 'tinyint': tipo_dato = sql.TinyInt; break;
                                                                case 'uniqueidentifier': tipo_dato = sql.UniqueIdentifier; break;
                                                                case 'varbinary': tipo_dato = sql.VarBinary; break;
                                                                case 'varchar': tipo_dato = sql.VarChar(parseInt(elemento.campos_tabla_olap[icampos].longitud)); break;
                                                                case 'xml': tipo_dato = sql.Xml; break;
                                                        }
                                                        
                                                        
                                                        // console.log(elemento.campos_tabla_olap[icampos].campo_destino, ' , ', tipo_dato,' , ', objeto[prop]);
                                                        
                                                        request.input(elemento.campos_tabla_olap[icampos].campo_destino, tipo_dato, objeto[prop]);
                                                } catch (error) {
                                                    mensaje = error.message;
                                                    console.log(mensaje)
                                                    break;
                                                }
                                                    icampos ++;
                                            }
    

                                            // console.log(`use ${dbdatos.databases.destino}; insert into ${elemento.destino} (${columnas}) values (${alias});`);
    
                                    
                                            try {
                                                const re = await request.query(`use ${dbdatos.databases.destino}; insert into ${elemento.destino} (${columnas}) values (${alias});`);
                                                filas_afectadas += parseInt(re.rowsAffected);
                                                mensaje = '_';
                                            } catch (error) {
                                                mensaje = error.message;
                                                
                                                break;
                                            }
                                    
                                            
                                    
                                }//2FOR CADA REGISTRO
                                if(mensaje != '_'){
                                    mensaje = 'ocurrio el siguiente error:  \n'  + mensaje
                                    etl_ejecutados.push({etl: `${elemento.destino}`, filas_afectadas, mensaje})
                                    console.log(mensaje);
                                    break;
                                }


                            
                    console.log('ETL terminado correctamente para la dimension: ', elemento.destino);
                    if(mensaje === '_') mensaje = 'ETL terminado correctamente!';
                    etl_ejecutados.push({etl: `${elemento.destino}`, filas_afectadas, mensaje})
                    }// 1FOR ETL------------------------------------------------------------------------------------------------------------------------

        } catch (error) {
            console.log(error)
            
        }

    }//1IF ------------------------------------------------------------------------------------------------------------------------------------------------------- 



    res.render('ejecutar_etl',{
        etl_ejecutados,
        mensaje
    });
}





module.exports = ejecutar_etl;

