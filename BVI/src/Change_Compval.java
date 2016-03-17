


import java.sql.*;
import java.io.*;
import java.util.*;
import java.util.Date;
import java.text.*;

import com.mtas.*;
import com.mtas.util.logWriter;
import com.mtas.util.readSettings;


public class Change_Compval
{
    public static void main( String [] args )
    {
        Connection prog_con1 = null;
		Connection prog_con2 = null;
		Statement prog_stmt1a = null;
		Statement prog_stmt1b = null;
		Statement prog_stmt2 = null;
		ResultSet prog_rs1a;
		ResultSet prog_rs1b;
		int comp=0;
		
		Calendar mod_date = Calendar.getInstance();
	    mod_date.setTime(new Date()); // Now use today date.
		Date wd_date;
		Date md= mod_date.getTime();
		boolean Looping=true;
		
		readSettings getParms = new readSettings();
		logWriter log = new logWriter();
		String logfile="bvitest.log";
		
		String SQL1a,SQL1b,SQL2;
				
		String date_rep;
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		String MTAS = "Mtas Updated : ";
        Calendar javaCalendar = null;
        javaCalendar = Calendar.getInstance();
		date_rep = javaCalendar.get(Calendar.DATE) + "/" + (javaCalendar.get(Calendar.MONTH) + 1) + "/" + javaCalendar.get(Calendar.YEAR); 
      	
		try {
			if (args.length != 1) {
				System.err.println("Usage: java colin2 <settings file>");
				System.exit(1);
			}
			getParms.read(args[0]);
			
			Class.forName ("openlink.jdbc3.Driver");
			SQL1a = "Select * from Company";
						
			prog_con1= DriverManager.getConnection(getParms.prog_jdbc_connection);
			prog_stmt1a=prog_con1.createStatement();
			prog_stmt1b=prog_con1.createStatement();
			
			prog_rs1a=prog_stmt1a.executeQuery(SQL1a);
						
			prog_con2= DriverManager.getConnection(getParms.prog_jdbc_connection2);
			prog_con2.setAutoCommit(false);
			prog_stmt2=prog_con2.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_UPDATABLE);
			
			SQL1b = "Update Compval set compvalparam1 = 'no' where compvalcode = 'MTCO0524'";
			prog_stmt2.executeUpdate(SQL1b);
			SQL1b = "Update Compval set compvalparam1 = 'no' where compvalcode = 'MTCO0525'";
			prog_stmt2.executeUpdate(SQL1b);
			SQL1b = "Update Compval set compvalparam1 = 'P' where compvalcode = 'MTCO0501'";
			prog_stmt2.executeUpdate(SQL1b);
			SQL1b = "Update Compval set compvalparam1 = '61' where compvalcode = 'MTCO0506'";
			prog_stmt2.executeUpdate(SQL1b);
			SQL1b = "Update Compval set compvalparam1 = '62' where compvalcode = 'MTCO0507'";
			prog_stmt2.executeUpdate(SQL1b);
			SQL1b = "Update Compval set compvalparam1 = 'rbpo153.p' where compvalcode = 'MTCO0513'";
			prog_stmt2.executeUpdate(SQL1b);
//			SQL1b = "Insert into syspara (code,data) VALUES ('POPFXP',258696)";
//			prog_stmt2.executeUpdate(SQL1b);
			prog_con2.commit();
		}		
		
		catch( Exception e )
		{
			try {
				e.printStackTrace();
			}
			catch (Exception f) 
			{
				f.printStackTrace();
			}	
		}
    }
}
    
