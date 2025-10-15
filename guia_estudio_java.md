# Guía de Estudio para Examen de Java - Nivel Básico

## 1. Introducción a Java y BlueJ

**BlueJ** es un entorno de desarrollo integrado (IDE) diseñado especialmente para principiantes en programación orientada a objetos.

- **Crear un nuevo proyecto**: `Project > New Project`
- **Crear una nueva clase**: Hacer clic en `New Class` y elegir el tipo (por ejemplo, "Class")
- **Compilar**: Botón `Compile` o `Ctrl+K`
- **Crear objeto**: Hacer clic derecho en la clase y seleccionar `new NombreClase(...)`
- **Llamar a métodos**: Hacer clic derecho en el objeto creado y seleccionar el método

---

## 2. Sintaxis Básica

### Impresión por consola
```java
// Imprimir con salto de línea
System.out.println("Hola mundo");

// Imprimir sin salto de línea
System.out.print("Hola ");
System.out.print("mundo");
```

### Declaración y asignación de variables
```java
// Sintaxis: tipo nombre = valor;
int edad = 25;
double precio = 19.99;
char letra = 'A';
boolean esValido = true;
String nombre = "Juan";
```

### Constantes
```java
final double PI = 3.1416;
```

---

## 3. Ingreso de Datos por Teclado

Para leer datos del usuario, usamos la clase `Scanner`:

```java
import java.util.Scanner;

public class Ejemplo {
    public static void main(String[] args) {
        Scanner teclado = new Scanner(System.in);
        
        // Leer un entero
        System.out.print("Ingrese su edad: ");
        int edad = teclado.nextInt();
        
        // Leer un double
        System.out.print("Ingrese su altura: ");
        double altura = teclado.nextDouble();
        
        // Leer un String (una palabra)
        System.out.print("Ingrese su nombre: ");
        String nombre = teclado.next();
        
        // Leer una línea completa (para frases)
        teclado.nextLine(); // Consumir el salto de línea pendiente
        System.out.print("Ingrese su dirección: ");
        String direccion = teclado.nextLine();
        
        teclado.close();
    }
}
```

---

## 4. Estructuras de Control

### Estructuras Secuenciales
Son instrucciones que se ejecutan una tras otra.

### Estructuras Alternativas (Condicionales)

#### if-else simple
```java
if (edad >= 18) {
    System.out.println("Eres mayor de edad");
} else {
    System.out.println("Eres menor de edad");
}
```

#### if-else anidado
```java
if (nota >= 9) {
    System.out.println("Sobresaliente");
} else if (nota >= 7) {
    System.out.println("Notable");
} else if (nota >= 5) {
    System.out.println("Aprobado");
} else {
    System.out.println("Desaprobado");
}
```

#### switch
```java
switch (dia) {
    case 1:
        System.out.println("Lunes");
        break;
    case 2:
        System.out.println("Martes");
        break;
    // ... más casos
    default:
        System.out.println("Día inválido");
}
```

---

## 5. Funciones (Métodos)

Los métodos permiten reutilizar código y organizar el programa.

### Método sin retorno (void)
```java
public static void saludar() {
    System.out.println("¡Hola!");
}
```

### Método con retorno
```java
public static int sumar(int a, int b) {
    return a + b;
}
```

### Llamada a métodos
```java
saludar(); // Llamada a método void
int resultado = sumar(5, 3); // Llamada a método con retorno
```

### Parámetros
- **Parámetros formales**: variables en la definición del método
- **Argumentos**: valores reales pasados al llamar al método

---

## 6. La Clase String

`String` es una clase que representa cadenas de texto.

### Creación de Strings
```java
String s1 = "Hola";
String s2 = new String("Mundo");
```

### Métodos útiles de String
```java
String texto = "Hola Mundo";

// Longitud
int longitud = texto.length(); // 10

// Obtener carácter en posición
char c = texto.charAt(0); // 'H'

// Subcadena
String sub = texto.substring(0, 4); // "Hola"

// Comparación
boolean iguales = texto.equals("Hola Mundo"); // true
boolean igualesIgnorandoMayus = texto.equalsIgnoreCase("HOLA MUNDO"); // true

// Concatenación
String nuevo = texto + "!"; // "Hola Mundo!"
String nuevo2 = texto.concat("!"); // "Hola Mundo!"

// Búsqueda
int posicion = texto.indexOf("Mundo"); // 5

// Reemplazo
String reemplazado = texto.replace("Mundo", "Java"); // "Hola Java"

// Convertir a mayúsculas/minúsculas
String mayus = texto.toUpperCase(); // "HOLA MUNDO"
String minus = texto.toLowerCase(); // "hola mundo"
```

---

## 7. Arreglos (Arrays) y ArrayList

### Arrays (Arreglos tradicionales)
Colección de elementos del mismo tipo con tamaño fijo.

```java
// Declaración y creación
int[] numeros = new int[5]; // Array de 5 enteros
String[] nombres = {"Ana", "Juan", "Pedro"};

// Acceso a elementos
numeros[0] = 10; // Asignar valor al primer elemento
int primerNumero = numeros[0]; // Obtener valor

// Recorrer con for
for (int i = 0; i < numeros.length; i++) {
    System.out.println(numeros[i]);
}

// Recorrer con for-each
for (String nombre : nombres) {
    System.out.println(nombre);
}
```

### ArrayList
Colección dinámica (tamaño variable) que pertenece al framework de colecciones.

```java
import java.util.ArrayList;

// Creación
ArrayList<String> lista = new ArrayList<String>();
// En Java 7+ se puede usar diamante:
ArrayList<String> lista2 = new ArrayList<>();

// Métodos útiles
lista.add("Ana");        // Añadir elemento
lista.add("Juan");
lista.size();            // Obtener tamaño
lista.get(0);            // Obtener elemento en posición 0
lista.set(0, "María");   // Reemplazar elemento en posición 0
lista.remove(0);         // Eliminar elemento en posición 0
lista.contains("Juan");  // Verificar si contiene un elemento
lista.clear();           // Vaciar la lista

// Recorrer con for-each
for (String nombre : lista) {
    System.out.println(nombre);
}
```

---

## 8. Fundamentos de Programación Orientada a Objetos (POO)

La POO se basa en cuatro pilares:

1. **Abstracción**: Representar entidades del mundo real como objetos
2. **Encapsulamiento**: Ocultar detalles internos y exponer solo lo necesario
3. **Herencia**: Crear nuevas clases basadas en clases existentes
4. **Polimorfismo**: Un objeto puede tomar muchas formas

---

## 9. Construcción de Clases

Una clase es una plantilla para crear objetos.

```java
// Definición de una clase
public class Persona {
    // Atributos (campos)
    private String nombre;
    private int edad;
    
    // Constructor
    public Persona(String nombre, int edad) {
        this.nombre = nombre;
        this.edad = edad;
    }
    
    // Métodos
    public void saludar() {
        System.out.println("Hola, soy " + nombre);
    }
}
```

### Crear objetos
```java
Persona persona1 = new Persona("Ana", 25);
persona1.saludar(); // "Hola, soy Ana"
```

---

## 10. Atributos, Métodos y Modificadores de Acceso

### Atributos
Variables que representan el estado de un objeto.

### Métodos
Funciones que definen el comportamiento de un objeto.

### Modificadores de Acceso
- **private**: Solo accesible dentro de la misma clase
- **public**: Accesible desde cualquier lugar
- **protected**: Accesible dentro del mismo paquete y subclases
- **(default)**: Sin modificador, accesible solo dentro del mismo paquete

### Ejemplo completo
```java
public class CuentaBancaria {
    // Atributo privado
    private double saldo;
    
    // Constructor público
    public CuentaBancaria(double saldoInicial) {
        this.saldo = saldoInicial;
    }
    
    // Método público para depositar
    public void depositar(double monto) {
        if (monto > 0) {
            saldo += monto;
        }
    }
    
    // Método público para obtener saldo (getter)
    public double getSaldo() {
        return saldo;
    }
    
    // Método privado (solo usado internamente)
    private boolean validarMonto(double monto) {
        return monto > 0;
    }
}
```

---

## 11. Documentación de Clases

Es buena práctica documentar el código usando comentarios de JavaDoc:

```java
/**
 * Representa una persona con nombre y edad.
 * 
 * @author Tu Nombre
 * @version 1.0
 */
public class Persona {
    /**
     * El nombre de la persona.
     */
    private String nombre;
    
    /**
     * Obtiene el nombre de la persona.
     * 
     * @return el nombre como String
     */
    public String getNombre() {
        return nombre;
    }
}
```

---

## 12. Representación de Datos

La representación de datos se refiere a cómo modelamos la información en nuestras clases.

### Ejemplo: Clase Fecha
```java
public class Fecha {
    private int dia;
    private int mes;
    private int anio;
    
    public Fecha(int dia, int mes, int anio) {
        this.dia = dia;
        this.mes = mes;
        this.anio = anio;
    }
    
    public String toString() {
        return dia + "/" + mes + "/" + anio;
    }
}
```

### Relación con ArrayList
Podemos usar colecciones para representar múltiples instancias:

```java
ArrayList<Persona> personas = new ArrayList<>();
personas.add(new Persona("Ana", 25));
personas.add(new Persona("Juan", 30));
```

---

## 13. Tipos de Datos Abstractos (TDA)

Un TDA define un conjunto de datos y las operaciones que se pueden realizar sobre ellos, sin especificar la implementación.

### Ejemplo: TDA Pila (Stack)
```java
public class Pila {
    private ArrayList<String> elementos;
    
    public Pila() {
        elementos = new ArrayList<>();
    }
    
    // Operación push: añadir elemento
    public void push(String elemento) {
        elementos.add(elemento);
    }
    
    // Operación pop: eliminar y devolver último elemento
    public String pop() {
        if (elementos.isEmpty()) {
            return null;
        }
        return elementos.remove(elementos.size() - 1);
    }
    
    // Operación peek: ver último elemento sin eliminarlo
    public String peek() {
        if (elementos.isEmpty()) {
            return null;
        }
        return elementos.get(elementos.size() - 1);
    }
    
    // Verificar si está vacía
    public boolean isEmpty() {
        return elementos.isEmpty();
    }
}
```

---

## 14. Relaciones entre Clases

### Asociación
Una clase utiliza a otra.

```java
public class Biblioteca {
    private ArrayList<Libro> libros;
    
    public Biblioteca() {
        libros = new ArrayList<>();
    }
    
    public void agregarLibro(Libro libro) {
        libros.add(libro);
    }
}
```

### Composición
Una clase contiene objetos de otra clase como parte esencial de sí misma.

```java
public class Persona {
    private NombreCompleto nombre; // Composición
    
    public Persona(String nombre, String apellido) {
        this.nombre = new NombreCompleto(nombre, apellido);
    }
}

public class NombreCompleto {
    private String nombre;
    private String apellido;
    
    public NombreCompleto(String nombre, String apellido) {
        this.nombre = nombre;
        this.apellido = apellido;
    }
}
```

### Herencia
Una clase hereda atributos y métodos de otra.

```java
// Clase base
public class Vehiculo {
    protected String marca;
    
    public Vehiculo(String marca) {
        this.marca = marca;
    }
    
    public void arrancar() {
        System.out.println("El vehículo arrancó");
    }
}

// Clase derivada
public class Auto extends Vehiculo {
    private int puertas;
    
    public Auto(String marca, int puertas) {
        super(marca); // Llamar al constructor de la clase base
        this.puertas = puertas;
    }
    
    // Sobrescritura de método
    @Override
    public void arrancar() {
        System.out.println("El auto " + marca + " arrancó");
    }
}
```

---

## 15. Excepciones

Las excepciones manejan errores en tiempo de ejecución.

### Tipos de excepciones
- **Checked**: Deben ser manejadas (ej: `IOException`)
- **Unchecked**: No es obligatorio manejarlas (ej: `ArithmeticException`, `NullPointerException`)

### Bloques try-catch
```java
try {
    // Código que puede lanzar una excepción
    int resultado = 10 / 0;
} catch (ArithmeticException e) {
    // Manejar la excepción
    System.out.println("Error: División por cero");
} finally {
    // Código que siempre se ejecuta (opcional)
    System.out.println("Finalizando operación");
}
```

### Lanzar excepciones
```java
public void dividir(int a, int b) {
    if (b == 0) {
        throw new IllegalArgumentException("El divisor no puede ser cero");
    }
    return a / b;
}
```

### Excepciones personalizadas
```java
public class EdadInvalidaException extends Exception {
    public EdadInvalidaException(String mensaje) {
        super(mensaje);
    }
}

// Uso
public void setEdad(int edad) throws EdadInvalidaException {
    if (edad < 0 || edad > 150) {
        throw new EdadInvalidaException("Edad fuera de rango válido");
    }
    this.edad = edad;
}
```

---

## Resumen de Conceptos Clave

| Concepto | Descripción |
|----------|-------------|
| **Clase** | Plantilla para crear objetos |
| **Objeto** | Instancia de una clase |
| **Atributo** | Variable que representa el estado |
| **Método** | Función que define el comportamiento |
| **Constructor** | Método especial para crear objetos |
| **Encapsulamiento** | Uso de modificadores de acceso |
| **Herencia** | Relación "es un" entre clases |
| **Polimorfismo** | Un objeto puede tener múltiples formas |
| **ArrayList** | Lista dinámica del framework de colecciones |
| **Excepción** | Manejo de errores en tiempo de ejecución |

¡Buena suerte en tu examen! Recuerda practicar escribiendo código y no solo leerlo.