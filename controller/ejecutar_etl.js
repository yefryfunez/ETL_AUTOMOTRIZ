const dbdatos = require('../conexion');
const ejecutar_etl = {};
const fs = require('fs');
const path = require('path');
const sql = require('mssql');
/*  ======================================================== CONSTANTES ===============================================================*/

let mensaje = '_';



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



 



/*  ======================================================== FUNCIONES ===============================================================*/










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
                    let i = 0;
                    
                    /* ************************************************************** EXTRACCION ***************************************************************** */
                    for(let elemento of dbdatos.lista_etl){
                    //dbdatos.lista_etl.forEach( async elemento => {// 1FOR ------------------------------------------------------------------------------------------------------------------------
                            
                            let datos = await pool.request().query(`use ${dbdatos.databases.origen}; ${elemento.consulta}`);//obtener los datos de la tabla origen especificada en el etl
                            datos = datos.recordset;












                    /* *********************************************************** TRANSFORMACION ***************************************************************** */
                            let datos_modificados = [];
                            datos.forEach(registro => {
                                let i=0;
                                let j=0;
                                let registro_modificado = {};
                                for(let p in registro){
                                    if(elemento.campos_tabla_olap[i].campo_origen === 'ninguno') i++;
                                    if(typeof registro[p] === 'string'){
                                            
                                            /* CONVIERTE EL DATO A MAYUSCULA O MINUSCULA SEGUN SE HAYA ELEGIDO */
                                            if(elemento.campos_tabla_olap[i].modificar === 'Mayuscula') registro[p] = registro[p].toUpperCase();
                                            if(elemento.campos_tabla_olap[i].modificar === 'Minuscula') registro[p] = registro[p].toLowerCase();

                                            /* CONCATENAR LOS CAMPOS SI HAY QUE CONCATENAR */
                                            if(elemento.campos_tabla_olap[i].concatenar === 'si'){
                                                    j++;
                                                    if(registro_modificado[elemento.campos_tabla_olap[i].campo_destino] === undefined) registro_modificado[elemento.campos_tabla_olap[i].campo_destino] = registro[p];
                                                    else registro_modificado[elemento.campos_tabla_olap[i].campo_destino] += ' ' + registro[p];

                                                    if(Array.isArray(elemento.campos_tabla_olap[i].campo_origen))
                                                        if(j < elemento.campos_tabla_olap[i].campo_origen.length ) i--;
                                                    
                                                        registro_modificado[elemento.campos_tabla_olap[i].campo_destino] = registro_modificado[elemento.campos_tabla_olap[i].campo_destino].substring(0,elemento.campos_tabla_olap[i].longitud)
                                            }else{
                                                    registro_modificado[elemento.campos_tabla_olap[i].campo_destino] = registro[p].substring(0,elemento.campos_tabla_olap[i].longitud).trim();

                                            }
                                    } else {
                                            registro_modificado[elemento.campos_tabla_olap[i].campo_destino] = registro[p];
                                    }
                                    i++;
                                }
                                datos_modificados.push(registro_modificado)
                            })
                            //console.log(datos_modificados);







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

                            



                            let data = '';
                            let tipo_dato = '';
                    /* ************************************************************** CARGA ***************************************************************** */

                                


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
                                                        
                                                        
                                                        //console.log(elemento.campos_tabla_olap[icampos].campo_destino, ' , ', tipo_dato,' , ', objeto[prop]);
                                                        
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
                                                await request.query(`use ${dbdatos.databases.destino}; insert into ${elemento.destino} (${columnas}) values (${alias});`);
                                                mensaje = '_';
                                            } catch (error) {
                                                mensaje = error.message;
                                                console.log(mensaje)
                                                break;
                                            }
                                            // console.log(re);
                                    
    
                                    
                                }//2FOR CADA REGISTRO
                                if(mensaje != '_'){
                                    mensaje = 'ocurrio el siguiente error en el etl para la dimension: ' + elemento.destino + '\n' + mensaje
                                    break;
                                }
/*                                 console.log(`¡ETL para la dimensión ${elemento.destino} completado con exito!`)
                                mensaje = `¡ETL para la dimensión ${elemento.destino} completado con exito!`; */

                            
                    console.log('ETL terminado correctamente para la dimension: ', elemento.destino);
                    }// 1FOR ETL------------------------------------------------------------------------------------------------------------------------
                    if(mensaje === '_') mensaje = 'ETL terminado correctamente!';
        } catch (error) {
            mensaje = 'Ocurio el siguiente error: \n\n', error.message;
            console.log(mensaje)
        }

    }//1IF ------------------------------------------------------------------------------------------------------------------------------------------------------- 



    res.render('ejecutar_etl',{
        etl_ejecutados,
        mensaje
    });
}





module.exports = ejecutar_etl;

