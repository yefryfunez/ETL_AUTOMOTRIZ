const fs = require('fs');
const path = require('path');

const filePath = path.join(__dirname,'ETL.json');








// Data to be saved
const data = {
  name: 'John Doe',
  age: 30,
  email: 'johndoe@example.com',
  arreglo: [{ala: 'ala', cola: 'cola'}, {ala:'ala2', cola: 'cola2'}],
  arreglo: [{nombre: 'Maria lopez', canciones_fav: ['always', 'beat it', 'let it be']}]
};

// Convert the data to JSON format
const jsonData = JSON.stringify(data);

// Specify the file path

// Write the JSON data to the file
const actualizar_etl = () => {
  fs.writeFile(filePath, jsonData, (err) => {
    if (err) {
      console.error('Error writing to file:', err);
      return;
    }
    console.log('Data has been saved to', filePath);
    return;
  });
}














const leer_archivo = () => {

fs.readFile(filePath, 'utf8', (err, data) => {
    if (err) {
      console.error('Error leyendo el archivo:', err);
      return;
    }
  
    try {
      // Analiza el contenido JSON en un objeto
      const jsonData = JSON.parse(data);
      console.log('Datos leídos del archivo:', jsonData);
    } catch (error) {
      console.error('Error al analizar JSON:', error);
    }
  });

}

module.exports = {leer_archivo, actualizar_etl};