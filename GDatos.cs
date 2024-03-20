using System;
using System.Data;
using System.Diagnostics;
using System.IO;

namespace Lexen.AccesoDatos
{
    /// <summary>
    /// Clase abstracta de persistencia de datos
    /// </summary>
    /// <remarks>
    /// Permite abstraer la capa de persistencia de datos del motor de datos subyacente
    /// </remarks>
    public abstract class GDatos
    {
        private enum DatabaseOperation
        {
            Init,
            Commit,
            Rollback,
            InitFail,
            CommitFail,
            RollbackFail,
            Execute,
            ExecuteSQL,
            ExecuteParams
        }


        #region "Declaración de Variables"

        /// <summary>
        /// Nombre del servidor de base de datos
        /// </summary>
        protected string MServidor = "";
        /// <summary>
        /// Nombre de la base de datos
        /// </summary>
        protected string MBase = "";
        /// <summary>
        /// Usuario de acceso a la base de datos
        /// </summary>
        protected string MUsuario = "";
        /// <summary>
        /// Contraseña de acceso a la base de datos
        /// </summary>
        protected string MPassword = "";
        /// <summary>
        /// Cadena de conexión a la base de datos
        /// </summary>
        protected string MCadenaConexion = "";
        /// <summary>
        /// Indica si para el acceso a la base de datos se utiliza autenticación Windows
        /// </summary>
        protected bool MWindows = false;
        /// <summary>
        /// Objeto de conexión con la base de datos. 
        /// </summary>
        protected IDbConnection MConexion;

        #endregion

        #region "Setters y Getters"

        /// <summary>
        /// Obtiene o establece el nombre del equipo servidor de datos. 
        /// </summary>
        public string Servidor
        {
            get { return MServidor; }
            set { MServidor = value; }
        }


        /// <summary>
        /// Obtiene o establece el nombre de la base de datos
        /// </summary>
        public string Base
        {
            get { return MBase; }
            set { MBase = value; }
        }

        /// <summary>
        /// Obtiene o establece el usuario de acceso a la base de datos
        /// </summary>
        public string Usuario
        {
            get { return MUsuario; }
            set { MUsuario = value; }
        }

        /// <summary>
        /// Obtiene o establece la contraseña de acceso a la base de datos
        /// </summary>
        public string Password
        {
            get { return MPassword; }
            set { MPassword = value; }
        } // end Password

        /// <summary>
        /// Obtiene o establece si usaremos la autenticación Windos para validarnos con la base de datos
        /// </summary>
        public bool Windows
        {
            get { return MWindows; }
            set { MWindows = value; }
        }

        /// <summary>
        /// Obtiene o establece la cadena de conexión completa con la base de datos
        /// </summary>
        public abstract string CadenaConexion
        { get; set; }

        /// <summary>
        /// Obtiene la conexión que se usa actualmente
        /// </summary>
        public IDbConnection ObjConexion
        {
            get
            {
                return MConexion;
            }

        }
        /// <summary>
        /// Propiedad para activar el fichero de log de transacciones
        /// </summary>
        public bool EnabledLog { get; set; }
        /// <summary>
        /// Fichero para el log de transacciones
        /// </summary>
        public string Logfile { get; set; }

        #endregion

        #region "Privadas"

        /// <summary>
        /// Crea u obtiene un objeto para conectarse a la base de datos
        /// </summary>
        protected IDbConnection Conexion
        {
            get
            {
                // si aun no tiene asignada la cadena de conexion lo hace
                if (MConexion == null)
                    MConexion = CrearConexion(CadenaConexion);

                // si no esta abierta aun la conexion, lo abre
                if (MConexion.State != ConnectionState.Open)
                    MConexion.Open();

                // retorna la conexion en modo interfaz, para que se adapte a cualquier implementacion de los distintos fabricantes de motores de bases de datos
                return MConexion;
            } // end get
        } // end Conexion

        #endregion

        #region "Lecturas"

        /// <summary>
        /// Obtiene un DataSet a partir de un Procedimiento Almacenado.
        /// </summary>
        /// <param name="procedimientoAlmacenado">Nombre del procedimiento almacenado</param>
        /// <returns>DataSet con el resultado</returns>
        public DataSet TraerDataSet(string procedimientoAlmacenado)
        {
            var mDataSet = new DataSet();
            CrearDataAdapter(procedimientoAlmacenado).Fill(mDataSet);
            return mDataSet;
        }

        /// <summary>
        /// Obtiene un DataSet a partir de un Procedimiento Almacenado y sus parámetros.
        /// </summary>
        /// <param name="procedimientoAlmacenado">Nombre del procedimiento almacenado</param>
        /// <param name="args">Lista de argumentos a pasar al procedimiento almacenado</param>
        /// <returns>DataSet con el resultado</returns>
        public DataSet TraerDataSet(string procedimientoAlmacenado, params Object[] args)
        {
            var mDataSet = new DataSet();
            CrearDataAdapter(procedimientoAlmacenado, args).Fill(mDataSet);
            return mDataSet;
        } // end TraerDataset

        /// <summary>
        /// Obtiene un DataSet a partir de un Query Sql.
        /// </summary>
        /// <param name="comandoSql">Comando SQL</param>
        /// <returns>DataSet con el resultado</returns>
        public DataSet TraerDataSetSql(string comandoSql)
        {
            var mDataSet = new DataSet();
            CrearDataAdapterSql(comandoSql).Fill(mDataSet);
            return mDataSet;
        } // end TraerDataSetSql

        /// <summary>
        /// Obtiene un DataTable a partir de un Procedimiento Almacenado.
        /// </summary>
        /// <param name="procedimientoAlmacenado">Nombre del procedimiento almacenado</param>
        /// <returns>DataTable con el resultado</returns>
        public DataTable TraerDataTable(string procedimientoAlmacenado)
        { return TraerDataSet(procedimientoAlmacenado).Tables[0].Copy(); } // end TraerDataTable

        /// <summary>
        /// Obtiene un DataTable a partir de un Procedimiento Almacenado y sus parámetros.
        /// </summary>
        /// <param name="procedimientoAlmacenado">Nombre del procedimiento almacenado</param>
        /// <param name="args">Lista de argumentos a pasar al procedimiento almacenado</param>
        /// <returns>DataTable con el resultado</returns>
        public DataTable TraerDataTable(string procedimientoAlmacenado, params Object[] args)
        { return TraerDataSet(procedimientoAlmacenado, args).Tables[0].Copy(); } // end TraerDataTable

        /// <summary>
        /// Obtiene un DataTable a partir de un Query SQL
        /// </summary>
        /// <param name="comandoSql">Comando SQL</param>
        /// <returns>DataTable con el resultado</returns>
        public DataTable TraerDataTableSql(string comandoSql)
        { return TraerDataSetSql(comandoSql).Tables[0].Copy(); } // end TraerDataTableSql

        /// <summary>
        /// Obtiene un DataReader a partir de un Procedimiento Almacenado.
        /// </summary>
        /// <param name="procedimientoAlmacenado">Nombre del procedimiento almacenado</param>
        /// <returns>DataReader con el resultado</returns>
        public IDataReader TraerDataReader(string procedimientoAlmacenado)
        {
            var com = Comando(procedimientoAlmacenado);
            return com.ExecuteReader();
        } // end TraerDataReader 

        /// <summary>
        /// Obtiene un DataReader a partir de un Procedimiento Almacenado y sus parámetros.
        /// </summary>
        /// <param name="procedimientoAlmacenado">Nombre del procedimiento almacenado</param>
        /// <param name="args">Lista de argumentos a pasar al procedimiento almacenado</param>
        /// <returns>DataReader con el resultado</returns>
        public IDataReader TraerDataReader(string procedimientoAlmacenado, params object[] args)
        {
            var com = Comando(procedimientoAlmacenado);
            CargarParametros(com, args);
            return com.ExecuteReader();
        } // end TraerDataReader

        /// <summary>
        /// Obtiene un DataReader a partir de un Procedimiento Almacenado.
        /// </summary>
        /// <param name="comandoSql">Comando SQL</param>
        /// <returns>DataReader con el resultado</returns>
        public IDataReader TraerDataReaderSql(string comandoSql)
        {
            var com = ComandoSql(comandoSql);
            return com.ExecuteReader();
        } // end TraerDataReaderSql 

        /// <summary>
        /// Obtiene un Valor Escalar a partir de un Procedimiento Almacenado. Solo funciona con SP's que tengan
        /// definida variables de tipo output, para funciones escalares mas abajo se declara un metodo
        /// </summary>
        /// <param name="procedimientoAlmacenado">Nombre del procedimiento almacenado</param>
        /// <returns>Valor de retorno del procedimiento almacenado</returns>
        public object TraerValorOutput(string procedimientoAlmacenado)
        {
            // asignar el string sql al command
            var com = Comando(procedimientoAlmacenado);
            // ejecutar el command
            com.ExecuteNonQuery();
            // declarar variable de retorno
            Object resp = null;

            // recorrer los parametros del SP
            foreach (IDbDataParameter par in com.Parameters)
                // si tiene parametros de tipo IO/Output retornar ese valor
                if (par.Direction == ParameterDirection.InputOutput || par.Direction == ParameterDirection.Output)
                    resp = par.Value;
            return resp;
        } // end TraerValor

        /// <summary>
        /// Obtiene un Valor a partir de un Procedimiento Almacenado, y sus parámetros.
        /// </summary>
        /// <param name="procedimientoAlmacenado">Nombre del procedimiento almacenado</param>
        /// <param name="args">Lista de argumentos a pasar al procedimiento almacenado</param>
        /// <returns>Valor de retorno del procedimiento almacenado</returns>
        public object TraerValorOutput(string procedimientoAlmacenado, params Object[] args)
        {
            // asignar el string sql al command
            var com = Comando(procedimientoAlmacenado);
            // cargar los parametros del SP
            CargarParametros(com, args);
            // ejecutar el command
            com.ExecuteNonQuery();
            // declarar variable de retorno
            Object resp = null;

            // recorrer los parametros del SP
            foreach (IDbDataParameter par in com.Parameters)
                // si tiene parametros de tipo IO/Output retornar ese valor
                if (par.Direction == ParameterDirection.InputOutput || par.Direction == ParameterDirection.Output)
                    resp = par.Value;
            return resp;
        } // end TraerValor

        /// <summary>
        /// Obtiene un Valor Escalar a partir de un comando SQL.
        /// </summary>
        /// <param name="comadoSql">Comando SQL</param>
        /// <returns>Valor de retorno del comando SQL</returns>
        public object TraerValorOutputSql(string comadoSql)
        {
            // asignar el string sql al command
            var com = ComandoSql(comadoSql);
            // ejecutar el command
            com.ExecuteNonQuery();
            // declarar variable de retorno
            Object resp = null;

            // recorrer los parametros del Query (uso tipico envio de varias sentencias sql en el mismo command)
            foreach (IDbDataParameter par in com.Parameters)
                // si tiene parametros de tipo IO/Output retornar ese valor
                if (par.Direction == ParameterDirection.InputOutput || par.Direction == ParameterDirection.Output)
                    resp = par.Value;
            return resp;
        } // end TraerValor

        /// <summary>
        /// Obtiene un vector de valores a partir de un Procedimiento Almacenado, y sus parámetros OUTPUT O INPUTOUTPUT.
        /// </summary>
        /// <param name="procedimientoAlmacenado">Nombre del procedimiento almacenado</param>
        /// <param name="args">Lista de argumentos a pasar al procedimiento almacenado</param>
        /// <returns>Vector de valores de retorno del procedimiento almacenado</returns>
        public object[] TraerValoresOutput(string procedimientoAlmacenado, params Object[] args)
        {
            // asignar el string sql al command
            var com = Comando(procedimientoAlmacenado);
            // cargar los parametros del SP
            CargarParametros(com, args);
            // ejecutar el command
            com.ExecuteNonQuery();
            // declarar variable de retorno
            Object[] resp = null;

            int num_outputs = 0;
            int indice = 0;
            foreach (IDbDataParameter par in com.Parameters)
                // si tiene parametros de tipo IO/Output retornar ese valor
                if (par.Direction == ParameterDirection.InputOutput || par.Direction == ParameterDirection.Output)
                    num_outputs++;
            // recorrer los parametros del SP
            resp = new Object[num_outputs];
            foreach (IDbDataParameter par in com.Parameters)
                // si tiene parametros de tipo IO/Output retornar ese valor
                if (par.Direction == ParameterDirection.InputOutput || par.Direction == ParameterDirection.Output)
                {
                    resp[indice] = par.Value;
                    indice++;
                }

            return resp;
        } // end TraerValores

        /// <summary>
        /// Obtiene un Valor Escalar a partir de un comando SQL.
        /// </summary>
        /// <param name="comadoSql">Comando SQL</param>
        /// <returns>Valor de retorno del comando SQL</returns>
        public object[] TraerValoresOutputSql(string comadoSql)
        {
            // asignar el string sql al command
            var com = ComandoSql(comadoSql);
            // ejecutar el command
            com.ExecuteNonQuery();
            // declarar variable de retorno
            Object[] resp = null;

            int num_outputs = 0;
            int indice = 0;
            foreach (IDbDataParameter par in com.Parameters)
                // si tiene parametros de tipo IO/Output retornar ese valor
                if (par.Direction == ParameterDirection.InputOutput || par.Direction == ParameterDirection.Output)
                    num_outputs++;
            // recorrer los parametros del SP
            resp = new Object[num_outputs];
            foreach (IDbDataParameter par in com.Parameters)
                // si tiene parametros de tipo IO/Output retornar ese valor
                if (par.Direction == ParameterDirection.InputOutput || par.Direction == ParameterDirection.Output)
                {
                    resp[indice] = par.Value;
                    indice++;
                }

            return resp;
        } // end TraerValores

        /// <summary>
        /// Obtiene un Valor de una funcion Escalar a partir de un Procedimiento Almacenado.
        /// </summary>
        /// <param name="procedimientoAlmacenado">Nombre del procedimiento almacenado</param>
        /// <returns>Valor obtenido de la primera fila y primera columna del procedimiento almacenado</returns>
        public object TraerValorEscalar(string procedimientoAlmacenado)
        {
            var com = Comando(procedimientoAlmacenado);
            return com.ExecuteScalar();
        } // end TraerValorEscalar

        /// <summary>
        /// Obtiene un Valor de una funcion Escalar a partir de un Procedimiento Almacenado, con Params de Entrada
        /// </summary>
        /// <param name="procedimientoAlmacenado">Nombre del procedimiento almacenado</param>
        /// <param name="args">Lista de argumentos a pasar al procedimiento almacenado</param>
        /// <returns>Valor obtenido de la primera fila y primera columna del procedimiento almacenado</returns>
        public Object TraerValorEscalar(string procedimientoAlmacenado, params object[] args)
        {
            var com = Comando(procedimientoAlmacenado);
            CargarParametros(com, args);
            object prueba = com.ExecuteScalar();
            return prueba;
        } // end TraerValorEscalar

        /// <summary>
        /// Obtiene un Valor de una funcion Escalar a partir de un Query SQL
        /// </summary>
        /// <param name="comandoSql">Comando SQL</param>
        /// <returns>Valor obtenido de la primera fila y primera columna del comando SQL</returns>
        public object TraerValorEscalarSql(string comandoSql)
        {
            var com = ComandoSql(comandoSql);
            return com.ExecuteScalar();
        } // end TraerValorEscalarSql

        #endregion

        #region "Acciones"

        /// <summary>
        /// Interfaz para crear la conexión de datos
        /// </summary>
        /// <param name="cadena">Cadena de conexión a la base de datos</param>
        /// <returns>Objeto conexión</returns>
        protected abstract IDbConnection CrearConexion(string cadena);
        /// <summary>
        /// Interfaz para un procedimiento almacenado
        /// </summary>
        /// <param name="procedimientoAlmacenado">Nombre del procedimiento almacenado</param>
        /// <returns>Objeto Command tipo procedimiento almacenado</returns>
        protected abstract IDbCommand Comando(string procedimientoAlmacenado);
        /// <summary>
        /// Interfaz para un comando SQL
        /// </summary>
        /// <param name="comandoSql">Comando SQL</param>
        /// <returns>Objeto Command del tipo sentencia sql</returns>
        protected abstract IDbCommand ComandoSql(string comandoSql);
        /// <summary>
        /// Interfaz para crear un DataAdapter
        /// </summary>
        /// <param name="procedimientoAlmacenado">Nombre del procedimiento almacenado</param>
        /// <param name="args">Lista de argumentos para el procedimiento almacenado</param>
        /// <returns>Objeto DataAdapter con objeto Command de tipo procedimiento almacenado</returns>
        protected abstract IDataAdapter CrearDataAdapter(string procedimientoAlmacenado, params Object[] args);
        /// <summary>
        /// Interfaz para crear un DataAdapter
        /// </summary>
        /// <param name="comandoSql">Comando SQL</param>
        /// <returns>Objeto DataAdapter con objeto Command de tipo sentencia SQL</returns>
        protected abstract IDataAdapter CrearDataAdapterSql(string comandoSql);
        /// <summary>
        /// Interfaz para rellenar parámetros en un objeto Command
        /// </summary>
        /// <param name="comando">Objeto Command</param>
        /// <param name="args">Lista de parámetros a agruegar</param>
        protected abstract void CargarParametros(IDbCommand comando, Object[] args);

        /// <summary>
        /// Metodo sobrecargado para autenticarse contra el motor de BBDD
        /// </summary>
        /// <returns>True si la autenticación es exitosa</returns>
        public bool Autenticar()
        {
            if (Conexion.State != ConnectionState.Open)
                Conexion.Open();
            return true;
        }// end Autenticar

        /// <summary>
        /// Metodo sobrecargado para autenticarse contra el motor de BBDD
        /// </summary>
        /// <param name="vUsuario">Usuario de la base de datos</param>
        /// <param name="vPassword">Constraseña de la base de datos</param>
        /// <returns>True si la autenticación es exitosa</returns>
        public bool Autenticar(string vUsuario, string vPassword)
        {
            MUsuario = vUsuario;
            MPassword = vPassword;
            MConexion = CrearConexion(CadenaConexion);

            MConexion.Open();
            return true;
        }// end Autenticar

        /// <summary>
        /// Cierra la conexión con la base de datos
        /// </summary>
        public void CerrarConexion()
        {
            if (Conexion.State != ConnectionState.Closed)
                MConexion.Close();
        }


        /// <summary>
        /// Ejecuta un Procedimiento Almacenado en la base de datos. 
        /// </summary>
        /// <param name="procedimientoAlmacenado">Nombre del procedimiento almacenado</param>
        /// <returns>¿?</returns>
        public int Ejecutar(string procedimientoAlmacenado)
        {
            if (EnabledLog) WriteLog(DatabaseOperation.Execute, procedimientoAlmacenado);
            return Comando(procedimientoAlmacenado).ExecuteNonQuery(); 
        } // end Ejecutar

        /// <summary>
        /// Ejecuta un query sql
        /// </summary>
        /// <param name="comandoSql">Comando sql</param>
        /// <returns>¿?</returns>
        public int EjecutarSql(string comandoSql)
        {
            if (EnabledLog) WriteLog(DatabaseOperation.ExecuteSQL, comandoSql);
            return ComandoSql(comandoSql).ExecuteNonQuery(); 
        } // end Ejecutar

        /// <summary>
        /// Ejecuta un Procedimiento Almacenado en la base, utilizando los parámetros. 
        /// </summary>
        /// <param name="procedimientoAlmacenado">Nombre del procedimiento almacenado</param>
        /// <param name="args">Lista de argumentos</param>
        /// <returns>¿?</returns>
        public int Ejecutar(string procedimientoAlmacenado, params Object[] args)
        {
            var com = Comando(procedimientoAlmacenado);
            CargarParametros(com, args);
            var resp = com.ExecuteNonQuery();
            for (var i = 0; i < com.Parameters.Count; i++)
            {
                var par = (IDbDataParameter)com.Parameters[i];
                if (par.Direction == ParameterDirection.InputOutput || par.Direction == ParameterDirection.Output)
                    args.SetValue(par.Value, i - 1);
            }// end for
            if (EnabledLog) WriteLog(DatabaseOperation.ExecuteParams, procedimientoAlmacenado);
            return resp;
        } // end Ejecutar

        /// <summary>
        /// Genera un insert genérico a una tabla con los campos que le pasamos como argumentos
        /// </summary>
        /// <param name="nombreTabla">Nombre de la tabla</param>
        /// <param name="args">Lista de los valores de los campos a insertar</param>
        /// <remarks>
        /// Deben incluirse todos los campos de la tabla
        /// </remarks>
        /// <returns>¿?</returns>
        public virtual string GeneraInsert(string nombreTabla, params Object[] args) { throw new NotImplementedException(); }
        /// <summary>
        /// Genera un delete genérico a una tabla para borrarla completamente
        /// </summary>
        /// <param name="nombreTabla">Nombre de la tabla</param>
        /// <returns>¿?</returns>
        public virtual string GeneraBorrarTablaCompleta(string nombreTabla) { throw new NotImplementedException(); }

        #endregion

        #region "Transacciones"

        /// <summary>
        /// Interfaz de Transacción de base de datos
        /// </summary>
        protected IDbTransaction MTransaccion;

        /// <summary>
        /// Variable que nos indica si existe alguna transacción en curso
        /// </summary>
        public bool EnTransaccion
        {
            get; set;
        }
        


        /// <summary>
        /// Comienza una Transacción en la base en uso. 
        /// </summary>
        public void IniciarTransaccion()
        {
            try
            {
                MTransaccion = Conexion.BeginTransaction();
                if (EnabledLog) WriteLog(DatabaseOperation.Init, "");
                EnTransaccion = true;
            }// end try
            catch (Exception ex)
            {
                if (EnabledLog) WriteLog(DatabaseOperation.InitFail, ex.Message);
                EnTransaccion = false;
            }
        }// end IniciarTransaccion

        /// <summary>
        /// Confirma la transacción activa. 
        /// </summary>
        public void TerminarTransaccion()
        {
            try
            { 
                MTransaccion.Commit();
                if (EnabledLog) WriteLog(DatabaseOperation.Commit, "");
            }
            catch (Exception ex)
            {
                if (EnabledLog) WriteLog(DatabaseOperation.CommitFail, ex.Message);
            }
            finally
            {
                MTransaccion = null;
                EnTransaccion = false;
            }// end finally
        }// end TerminarTransaccion

        /// <summary>
        /// Cancela la transacción activa.
        /// </summary>
        public void AbortarTransaccion()
        {
            try
            { 
                MTransaccion.Rollback();
                if (EnabledLog) WriteLog(DatabaseOperation.Rollback, "");
            }
            catch (Exception ex)
            {
                if (EnabledLog) WriteLog(DatabaseOperation.RollbackFail, ex.Message);
            }
            finally
            {
                MTransaccion = null;
                EnTransaccion = false;
            }// end finally
        }// end AbortarTransaccion

        /// <summary>
        /// Escribe en el log de transacciones la operación realizada
        /// </summary>
        private void WriteLog(DatabaseOperation op, string msg)
        {
            using (StreamWriter sw = File.AppendText(Logfile))
            {
                try
                {
                    sw.WriteLine("{0} - {1} - {2} [{3}]", DateTime.Now.ToString(), op.ToString(), new StackFrame(2, true).GetMethod().Name, msg);
                }
                catch { }
            }
        }

        #endregion
    }

}
