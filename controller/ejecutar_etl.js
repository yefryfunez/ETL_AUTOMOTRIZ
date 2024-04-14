const dbdatos = require('../conexion');
const ejecutar_etl = {};
const fs = require('fs');
const path = require('path');
const sql = require('mssql');
/*  ======================================================== CONSTANTES ===============================================================*/

let mensaje = '_';
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
                    dbdatos.lista_etl.forEach( async elemento => {// 1FOR ------------------------------------------------------------------------------------------------------------------------
                            
                            let resultado = [];
                            
                            etl_ejecutados.push({destino: elemento.destino, estado: ''})

                            resultado = await pool.request().query(`use ${dbdatos.databases.origen}; ${elemento.consulta}`);//obtener los datos de la tabla origen especificada en el etl
                            resultado = resultado.recordset;





                    /* *********************************************************** TRANSFORMACION ***************************************************************** */
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





                        //console.log(`use ${dbdatos.databases.destino};`)
                        
                        //await pool.request().query(`use ${dbdatos.databases.destino};`)
                        //console.log(resultado)
                    
                    /* ************************************************************** CARGA ***************************************************************** */
                            resultado.forEach(async objeto=>{
                                let icampos = 0;
                                const request = pool.request();
                                        for(let prop in objeto){
                                            if(elemento.campos_tabla_olap[icampos].campo_origen === 'ninguno') icampos++;
                                            let data = '';
                                            let tipo_dato = '';
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
                                            
                                            if(typeof objeto[prop] === 'string') data = objeto[prop].trim();
                                            else data = parseFloat(objeto[prop])+'';
                                            

                                            if(elemento.campos_tabla_olap[icampos].modificar === 'Mayuscula'){
                                                data = data.toUpperCase();
                                            } else if(elemento.campos_tabla_olap[icampos].modificar === 'Minuscula'){
                                                data = data.toLowerCase();
                                            }

                                            //console.log(elemento.campos_tabla_olap[icampos].campo_destino, ' , ', tipo_dato,' , ', data,'jaja' );
                                            request.input(elemento.campos_tabla_olap[icampos].campo_destino, tipo_dato, data);
                                            icampos ++;
                                        }

                                
                                //console.log(`use ${dbdatos.databases.destino}; insert into ${elemento.destino} (${columnas}) values (${alias});`);
                                const re = await request.query(`use ${dbdatos.databases.destino}; insert into ${elemento.destino} (${columnas}) values (${alias});`);
                                
                                

                                
                            })

                    
                    console.log(`¡ETL para la dimensión ${elemento.destino} completado con exito!`)
                    mensaje = `¡ETL para la dimensión ${elemento.destino} completado con exito!`;

                    etl_ejecutados[i].estado = 'ETL terminado correctamente!';
                    console.log(etl_ejecutados[i])
                    i++;
                    })// 1FOR ------------------------------------------------------------------------------------------------------------------------
        } catch (error) {
            mensaje = 'Ocurio el siguiente error: \n', error;
        }

    }//1IF ------------------------------------------------------------------------------------------------------------------------------------------------------- 



    res.render('ejecutar_etl',{
        etl_ejecutados,
        mensaje
    });
}





module.exports = ejecutar_etl;