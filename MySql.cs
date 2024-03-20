using Lexen.AccesoDatos;
using MySqlConnector;
using System;
using System.Data;

namespace Lexen.AccesoDatos
{
    public class MySql : GDatos
    {
        public override string CadenaConexion {
            get 
            {
                if (MCadenaConexion.Length == 0)
                {

                    if (MBase.Length != 0 && MServidor.Length != 0)
                    {
                        var sCadena = new System.Text.StringBuilder("");
                        sCadena.AppendFormat("Server= \"{0}\";", Servidor);
                        sCadena.Append("Port=3306;");
                        sCadena.AppendFormat("Database=\"{0}\";", Base);
                        sCadena.AppendFormat("Uid=\"{0}\";", Usuario);
                        sCadena.AppendFormat("Pwd=\"{0}\";", Password);
                        //sCadena.Append("SslMode = Required;");
                        sCadena.Append("Convert Zero Datetime = True;");
                        return sCadena.ToString();
                    }

                    throw new Exception("No se puede establecer la cadena de conexión en la clase DatosSQLServer");
                }
                return MCadenaConexion = CadenaConexion;
            } 
            set { MCadenaConexion = value; }  
        }

        //public override string GeneraBorrarTablaCompleta(string nombreTabla)
        //{
        //    return string.Format("DELETE FROM {0}", nombreTabla);

        //}

        //public override string GeneraInsert(string nombreTabla, params object[] args)
        //{
        //    throw new NotImplementedException();
        //}

        protected override void CargarParametros(IDbCommand comando, object[] args)
        {
            for (int i = 0; i < comando.Parameters.Count; i++)
            {
                var p = (MySqlParameter)comando.Parameters[i];
                p.Value = i <= args.Length ? args[i] ?? DBNull.Value : null;
            } // end for
        }

        protected override IDbCommand Comando(string procedimientoAlmacenado)
        {

            MySqlCommand com;
            //if (ColComandos.Contains(procedimientoAlmacenado))
            //    com = (System.Data.SqlClient.SqlCommand)ColComandos[procedimientoAlmacenado];
            //else
            //{
            var con2 = new MySqlConnection(CadenaConexion);
            con2.Open();
            com = new MySqlCommand(procedimientoAlmacenado, con2) { CommandType = System.Data.CommandType.StoredProcedure };

            MySqlCommandBuilder.DeriveParameters(com);
            con2.Close();
            con2.Dispose();
            //ColComandos.Add(procedimientoAlmacenado, com);
            //}//end else
            com.Connection = Conexion as MySqlConnection;
            com.Transaction = (MySqlTransaction)MTransaccion;
            return com;
        }

        protected override IDbCommand ComandoSql(string comandoSql)
        {
            var com = new MySqlCommand(comandoSql, (MySqlConnection)Conexion, (MySqlTransaction)MTransaccion);
            com.Transaction = (MySqlTransaction) MTransaccion;
            return com;
        }

        protected override IDbConnection CrearConexion(string cadena)
        { return new MySqlConnection(cadena); }


        protected override IDataAdapter CrearDataAdapter(string procedimientoAlmacenado, params object[] args)
        {
            var da = new MySqlDataAdapter((MySqlCommand)Comando(procedimientoAlmacenado));
            if (args.Length != 0)
                CargarParametros(da.SelectCommand, args);
            return da;
        }

        protected override IDataAdapter CrearDataAdapterSql(string comandoSql)
        {
            var da = new MySqlDataAdapter((MySqlCommand)ComandoSql(comandoSql));
            return da;
        }

        private string FormaCampo(object valor)
        {
            string devolver = "";

            switch (valor.GetType().Name.ToLower())
            {
                case "sbyte":
                case "short":
                case "int":
                case "long":
                case "byte":
                case "ushort":
                case "uint":
                case "ulong":
                    devolver = valor.ToString();
                    break;
                case "float":
                case "double":
                case "decimal":
                    devolver = string.Format("{0}", valor.ToString().Replace(".", "").Replace(",", "."));
                    break;
                case "char":
                case "string":
                case "String":
                    devolver = string.Format("'{0}'", valor);
                    break;
                case "bool":
                case "boolean":
                    if ((bool)valor)
                    {
                        devolver = "1";
                    }
                    else
                    {
                        devolver = "0";
                    }
                    break;
                case "DateTime":
                case "datetime":
                    DateTime dt = (DateTime)valor;
                    devolver = string.Format("'{0}'", dt.Year.ToString() + dt.Month.ToString().PadLeft(2, '0') + dt.Day.ToString().PadLeft(2, '0') + " " + dt.Hour.ToString().PadLeft(2, '0') + ":" + dt.Minute.ToString().PadLeft(2, '0') + ":" + dt.Second.ToString().PadLeft(2, '0') + "." + dt.Millisecond.ToString().PadLeft(3, '0'));
                    break;
                default:
                    devolver = valor.ToString();
                    break;
            }

            return devolver;
        }

        public MySql()
        {
            Base = "";
            Servidor = "";
            Usuario = "";
            Password = "";
        }// end DatosMySql


        public MySql(string cadenaConexion)
        { CadenaConexion = cadenaConexion; }// end DatosMySql


        public MySql(string servidor, string @base)
        {
            Base = @base;
            Servidor = servidor;
        }// end DatosMySql


        public MySql(string servidor, string @base,string usuario, string password)
        {
            Base = @base;
            Servidor = servidor;
            Usuario = usuario;
            Password = password;
        }// end DatosMySql
    }
}
