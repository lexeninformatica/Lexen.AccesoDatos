namespace Lexen.AccesoDatos
{
    /// <summary>
    /// Clase de persistencia de datos
    /// </summary>
    public static class Conexion
    {
        /// <summary>
        /// Variable global que abstrae la conexión a un motor de base de datos
        /// </summary>
        public static GDatos GDatos;

        public static GDatos GDatosOrig;

        public static GDatos GDatosAux;

        public static bool Conectado;

        public static bool Auxiliar;

        public static string Usuario;
        // INICIAR SESION PARA SQL SERVER CE
        /*
        public static bool IniciarSesion(string cadenaConexion)
        {
            GDatos = new SqlServerCe(cadenaConexion);
            return GDatos.Autenticar();
        }
        public static bool IniciarSesionSQLServerCE(string nombreServidor, string baseDatos, string usuario, string password)
        {
            GDatos = new SqlServerCe(nombreServidor, baseDatos);
            return GDatos.Autenticar();
        } //fin inicializa sesion
        */

        /// <summary>
        /// Inicio de sesión para el motor SQL Server
        /// </summary>
        /// <param name="nombreServidor">Nombre del servidor con instancia si es necesario</param>
        /// <param name="baseDatos">Nombre de la base de datos</param>
        /// <param name="windows">True si usa autenticación Windows</param>
        /// <param name="usuario">Usuario de la base de datos</param>
        /// <param name="password">Contraseña de la base de datos</param>
        /// <returns>Booleano que indica si se ha validado el usuario en la base de datos o no</returns>
        public static bool IniciarSesionSQLServer(string nombreServidor, string baseDatos, bool windows, string usuario, string password)
        {
            GDatos = new SqlServer(nombreServidor, baseDatos, windows, usuario, password);
            Conectado = GDatos.Autenticar();
            return Conectado;
        }

        /// <summary>
        /// Inicio de sesión para el motor MySQL
        /// </summary>
        /// <param name="nombreServidor">Nombre del servidor con instancia si es necesario</param>
        /// <param name="baseDatos">Nombre de la base de datos</param>
        /// <param name="usuario">Usuario de la base de datos</param>
        /// <param name="password">Contraseña de la base de datos</param>
        /// <returns>Booleano que indica si se ha validado el usuario en la base de datos o no</returns>
        public static bool IniciarSesionMySql(string nombreServidor, string baseDatos, string usuario, string password)
        {
            GDatos = new MySql(nombreServidor, baseDatos, usuario, password);
            Conectado = GDatos.Autenticar();
            return Conectado;
        }
        /// <summary>
        /// Inicio de sesión para el motor SQL Server en la conexión auxiliar
        /// </summary>
        /// <param name="nombreServidor">Nombre del servidor con instancia si es necesario</param>
        /// <param name="baseDatos">Nombre de la base de datos</param>
        /// <param name="windows">True si usa autenticación Windows</param>
        /// <param name="usuario">Usuario de la base de datos</param>
        /// <param name="password">Contraseña de la base de datos</param>
        /// <returns>Booleano que indica si se ha validado el usuario en la base de datos o no</returns>
        public static bool IniciarSesionSQLServerAux(string nombreServidor, string baseDatos, bool windows, string usuario, string password)
        {
            GDatosAux = new SqlServer(nombreServidor, baseDatos, windows, usuario, password);
            return GDatosAux.Autenticar();
        }

        /// <summary>
        /// Inicio de sesión para el motor MySQL en la conexión auxiliar
        /// </summary>
        /// <param name="nombreServidor">Nombre del servidor con instancia si es necesario</param>
        /// <param name="baseDatos">Nombre de la base de datos</param>
        /// <param name="usuario">Usuario de la base de datos</param>
        /// <param name="password">Contraseña de la base de datos</param>
        /// <returns>Booleano que indica si se ha validado el usuario en la base de datos o no</returns>
        public static bool IniciarSesionMySqlAux(string nombreServidor, string baseDatos, string usuario, string password)
        {
            GDatosAux = new MySql(nombreServidor, baseDatos, usuario, password);
            return GDatosAux.Autenticar();
        }

        /// <summary>
        /// Cierra la conexión con la base de datos
        /// </summary>
        public static void FinalizarSesion()
        {
            GDatos.CerrarConexion();
        }

        /// <summary>
        /// Cierra la conexión con la base de datos
        /// </summary>
        public static void FinalizarSesionAux()
        {
            GDatosAux.CerrarConexion();
        }

        /// <summary>
        /// Se cambia la conexión principal por la conexión auxiliar activando la variable booleana Auxiliar para reflejar este cambio
        /// </summary>
        public static void CambiarConexion()
        {
            if(Auxiliar)
            {
                GDatos = GDatosOrig;
                Auxiliar = false;
            }
            else
            {
                GDatos = GDatosAux;
                Auxiliar = true;
            }
        }
    }
}
