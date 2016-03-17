


import java.sql.*;
import java.io.*;
import java.util.*;
import java.util.Date;
import java.text.*;

import com.mtas.*;
import com.mtas.util.logWriter;
import com.mtas.util.readSettings;


public class BVITest
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
			SQL1b = "Delete  from Company";
			prog_stmt2.executeUpdate(SQL1b);
			SQL1b = "Delete  from Complayhd";
			prog_stmt2.executeUpdate(SQL1b);
			SQL1b = "Delete  from Compval";
			prog_stmt2.executeUpdate(SQL1b);
			SQL1b = "Delete  from Complaydet";
			prog_stmt2.executeUpdate(SQL1b);
			prog_rs1a=prog_stmt1a.executeQuery(SQL1a);
			while (prog_rs1a.next())
				{ 
					wd_date=prog_rs1a.getDate("createddate");
					Date d=wd_date;
					SQL2="insert into company (companyid,Companyname,companyno,createdby,createddate) values ("+comp+",'BVI','BVI',"+prog_rs1a.getInt("createdby")+",{d '"+d+ "'})";
					System.out.println ("insert  "+SQL2);
					prog_stmt2.executeUpdate(SQL2);
					SQL2="insert into complaydet (companyid,complayhdid) values ("+comp+","+comp+")";
					prog_stmt2.executeUpdate(SQL2);
					SQL2="insert into complayhd (complayhdid,complayname) values ("+comp+",'BVI')";
					prog_stmt2.executeUpdate(SQL2);
								
					prog_con2.commit();
				}
			
			SQL1a = "Select * from Compval";
			prog_rs1a=prog_stmt1a.executeQuery(SQL1a);
			while (prog_rs1a.next())
				{ 
				    String P1="";
				    String P2="";
				    String P3="";
				    String P4="";
				    String P5="";
				    String P6="";
				    String P7="";
				    String P8="";		
				    int I1 =0;
				    if (prog_rs1a.getString("compvalparam1")!=null) {
				    	P1 = "'"+prog_rs1a.getString("compvalparam1")+"'" ;
				    }
				    if (prog_rs1a.getString("compvalparam2")!=null) {
				    	P2 = "'"+prog_rs1a.getString("compvalparam2")+"'" ;
				    }
				    if (prog_rs1a.getString("compvalparam3")!=null) {
				    	P3 = "'"+prog_rs1a.getString("compvalparam3")+"'" ;
				    }
				    if (prog_rs1a.getString("extrachar1")!=null) {
				    	P4 = "'"+prog_rs1a.getString("extrachar1")+"'" ;
				    }
				    if (prog_rs1a.getString("extrachar2")!=null) {
				    	P5 = "'"+prog_rs1a.getString("extrachar2")+"'" ;
				    }
				    
				    P6 = "'"+prog_rs1a.getString("compvalcode")+"'" ;
				    P7 = "'"+prog_rs1a.getString("compvaldesc").replace("'","''")+"'" ;
				//    P6 = "'"+prog_rs1a.getString("compvalcode")+"'" ;
		//			SQL2="insert into compval (companyid,compvalcode,compvaldesc,compvalid,compvalparam1,compvalparam2,compvalparam3,extrachar1,extrachar2,indexint1) values ("+prog_rs1a.getInt("companyid")+","+prog_rs1a.getString("compvalcode")+","+prog_rs1a.getString("compvaldesc")+","+prog_rs1a.getInt("compvalid")+","+P1+","+P2+","+P3+","+P4+","+P5+","+prog_rs1a.getInt("indexint1")+")";
				    SQL2="insert into compval (companyid,compvalcode,compvaldesc,compvalid,compvalparam1,compvalparam2,compvalparam3,extrachar1,extrachar2,indexint1) values ("+comp+","+P6+","+P7+","+prog_rs1a.getInt("compvalid")+","+P1+","+P2+","+P3+","+P4+","+P5+","+prog_rs1a.getInt("indexint1")+")";
				    System.out.println ("insert  "+SQL2);
					prog_stmt2.executeUpdate(SQL2);
				}	
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
		finally
		{
			try
			{
				prog_stmt1a.close();
				prog_stmt1b.close();
				prog_stmt2.close();
				prog_con1.close();
				prog_con2.close();;
			}
			catch( Exception e )
			{
				System.err.println( e );
			}
		}	
    }
}