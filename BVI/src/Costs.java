

import java.sql.*;
import java.sql.Date;
import java.io.*;
import java.math.BigDecimal;
import java.util.*;
import java.text.*;

import com.mtas.*;
import com.mtas.util.logWriter;
import com.mtas.util.readSettings;


public class Costs 
{
    public static void main( String [] args )
    {
        Connection prog_con1 = null;
		Connection prog_con2 = null;
		Connection sql_con = null;
		Statement sql_stmt = null;
		Statement prog_stmt1a = null;
		Statement prog_stmt1b = null;
		Statement prog_stmt1c = null;
		Statement prog_stmt1d = null;
		Statement prog_stmt2 = null;
		ResultSet prog_rs1a;
		ResultSet prog_rs1b;
		ResultSet prog_rs1c;
		ResultSet prog_rs1d;
		BigDecimal Labour_Cost;
		BigDecimal Overtime_Cost;
		BigDecimal Total_Cost;
		BigDecimal Material_Cost;
	    BigDecimal zero = BigDecimal.ZERO;
		boolean Looping=true;
		
		readSettings getParms = new readSettings();
		logWriter log = new logWriter();
		String logfile="colin1.log";
		
		String Rec_Type,Wrec_Typ,Trade_Code,Surname,Forename,Name,Type_Name,Mach_Name,Mach_No,Mach_Loc,Year,Month,Mach_Type,Mach_Type_Name;
		Date Wrec_Dte_Rep;
		String SQL1a,SQL1b,SQL1c,SQL1d,SQL2;
				
		int line_item,Wrec_Id;
		String date_com;
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Calendar javaCalendar = null;
        javaCalendar = Calendar.getInstance();
		date_com = javaCalendar.get(Calendar.DATE) + "/" + (javaCalendar.get(Calendar.MONTH) + 1) + "/" + javaCalendar.get(Calendar.YEAR); 
   
        java.util.Date date_repnew = javaCalendar.getTime();
        DateFormat DF1=new SimpleDateFormat("yyyy-MM-dd");
        String time_rep = "08:00";
		try {
			if (args.length != 1) {
				System.err.println("Usage: java colin1 <settings file>");
				System.exit(1);
			}
			getParms.read(args[0]);
			
			Class.forName ("openlink.jdbc3.Driver");
			Class.forName ("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			sql_con= DriverManager.getConnection(getParms.sqls_connection);
		    sql_con.setAutoCommit(false);
		    sql_stmt=sql_con.createStatement();
			 
			SQL1a = "delete from Bvicosts";
			sql_stmt.execute(SQL1a);
			sql_con.commit();
			SQL1a = "Select wrec_id,mach_id,wrec_typ,wrec_dte_rep from workrec where wrec_dte_rep  > {d '2010-01-01'}  order by wrec_id asc";
	
			log.write(logfile,"Starting job\r\n");
			String csv = "C:\\mtas\\output.csv";
			FileWriter writer = new FileWriter(csv);
			writer.append("Record Type");
		    writer.append('#');
		    writer.append("Year");
		    writer.append('#');
		    writer.append("Month");
		    writer.append('#');
		    writer.append("Work Record");
		    writer.append('#');
		    writer.append("Labour Cost");
		    writer.append('#');
		    writer.append("Overtime Cost");
		    writer.append('#');
		    writer.append("Total Cost");
		    writer.append('#');
		    writer.append("Material Cost");
		    writer.append('#');
		    writer.append("Asset Name");
		    writer.append('#');
		    writer.append("Asset No");
		    writer.append('#');
		    writer.append("Year");
		    writer.append('#');
		    writer.append("Asset Location");
		    writer.append('#');
		    writer.append("Asset Type");
		    writer.append('#');
		    writer.append("Empleyee name");
		    writer.append('#');
		    writer.append("Employee Trade");
		    writer.append('\n');
		  
			while (Looping)
			{
				Looping=false;
				prog_con1= DriverManager.getConnection(getParms.prog_jdbc_connection);
				prog_stmt1a=prog_con1.createStatement();
				prog_stmt1b=prog_con1.createStatement();
				prog_stmt1c=prog_con1.createStatement();
				prog_stmt1d=prog_con1.createStatement();
				prog_rs1a=prog_stmt1a.executeQuery(SQL1a);
				
				prog_con2= DriverManager.getConnection(getParms.prog_jdbc_connection);
				prog_con2.setAutoCommit(false);
				prog_stmt2=prog_con2.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_UPDATABLE);		
				Rec_Type = "Labour";
				Material_Cost = zero;
				
				while (prog_rs1a.next()) 
				{     
				//	Looping=true;
					
			//		System.out.println ("  Date "+prog_rs1a.getDate("wrec_dte_rep"));
			//		SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-mm-dd");
					SimpleDateFormat DATE_FORMAT_MM = new SimpleDateFormat("MM");
					SimpleDateFormat DATE_FORMAT_YY = new SimpleDateFormat("yyyy");
					Wrec_Dte_Rep = prog_rs1a.getDate("wrec_dte_rep");
					Month = DATE_FORMAT_MM.format(Wrec_Dte_Rep);
					Year = DATE_FORMAT_YY.format(Wrec_Dte_Rep);
					Wrec_Id = prog_rs1a.getInt("wrec_Id");
		//			Year = cast(datepart(year,Wrec_Dte_Rep) as varchar(4));
					
					SQL1b = "Select scu_det from syscomu where scu_no ='"+prog_rs1a.getString("wrec_typ")+"'";
				
					prog_rs1b=prog_stmt1b.executeQuery(SQL1b);  
					prog_rs1b.next();
					Wrec_Typ = prog_rs1b.getString("scu_det");
					if (+prog_rs1a.getInt("mach_id") > 0)
					{
						SQL1b = "Select mach_name,mach_no,mach_loc,mach_type from machdet where mach_id = "+prog_rs1a.getInt("mach_id");
			//			System.out.println (" "+prog_rs1a.getInt("mach_id"));
						prog_rs1b=prog_stmt1b.executeQuery(SQL1b);  
						if (prog_rs1b.next()) {
						
							Mach_Name = prog_rs1b.getString("mach_name");
							Mach_No = prog_rs1b.getString("mach_no");
							Mach_Loc = prog_rs1b.getString("mach_loc");
							Mach_Type = prog_rs1b.getString("mach_type");
						} else {
							Mach_Name = " ";
							Mach_No = " ";
							Mach_Loc = " ";
							Mach_Type = " ";
						}
					} else {
							Mach_Name = " ";
							Mach_No = " ";
							Mach_Loc = " ";
							Mach_Type = " ";
						}
					SQL1b = "Select type_name from assform where mach_type = '"+Mach_Type+"'";
					prog_rs1b=prog_stmt1b.executeQuery(SQL1b);  
					if (prog_rs1b.next()) {
					   Type_Name = prog_rs1b.getString("type_name");
					} else {
						Type_Name = " ";
					}
					SQL1b = "Select labour_cost,overtime_cost,trade_id,personnel_id from workcost where wrec_id = "+Wrec_Id;
					prog_rs1b=prog_stmt1b.executeQuery(SQL1b);  
					while (prog_rs1b.next())
					{
						Labour_Cost  = prog_rs1b.getBigDecimal("labour_cost");
						Overtime_Cost = prog_rs1b.getBigDecimal("overtime_cost");
						Total_Cost = Labour_Cost.add(Overtime_Cost);
						SQL1c = "Select scu_det from syscomu where scu_no ='"+prog_rs1b.getString("trade_id")+"'";
						prog_rs1c=prog_stmt1c.executeQuery(SQL1c);  
					//	prog_rs1c.next();
						if (prog_rs1c.next()) {
							Trade_Code = prog_rs1c.getString("scu_det");
						} else {
							Trade_Code = " ";
						}
						SQL1c = "Select wper_per_id from workper where wper_id ="+prog_rs1b.getString("personnel_id");
						prog_rs1c=prog_stmt1c.executeQuery(SQL1c);  
				//		prog_rs1c.next();
						if (prog_rs1c.next()) {
					
							if (+prog_rs1c.getInt("wper_per_id") > 0)
								{
								SQL1d = "Select perd_Sur,perd_frs from persdet where perd_id ="+prog_rs1c.getInt("wper_per_id");
								prog_rs1d=prog_stmt1d.executeQuery(SQL1d);  
								if (prog_rs1d.next()) {
									Surname = prog_rs1d.getString("perd_Sur");
									Forename = prog_rs1d.getString("perd_frs");
									Name = Forename + " " + Surname;
								} else {
									Surname = " ";
									Forename = " ";
									Name = " ";
								}		
							} else {
								Surname = " ";
								Forename = " ";
								Name = " ";
							}
						} else {
							Surname = " ";
							Forename = " ";
							Name = " ";
						}
						SQL1c = "Select tran_value from transrec where wrec_id ="+Wrec_Id;
						prog_rs1c=prog_stmt1c.executeQuery(SQL1c);  
						if (prog_rs1c.next()) {
							Material_Cost = Material_Cost.add(prog_rs1c.getBigDecimal("tran_value"));
						}
					System.out.println("Wrec Id = "+Wrec_Id+"  Date  "+Wrec_Dte_Rep+"  Year  "+Year+"  Month  "+Month+"  Labour  "+Labour_Cost+"  Overtime  "+Overtime_Cost+"  Trade  "+Trade_Code+"   Machine  "+Mach_No+ "  Name "+ Forename+" "+Surname);
						writer.append(Rec_Type);
					    writer.append('#');
					    writer.append(Year);
					    writer.append('#');
					    writer.append(Month);
					    writer.append('#');
					    writer.append(String.valueOf(Wrec_Id));
					    writer.append('#');
					    writer.append(String.valueOf(Labour_Cost));
					    writer.append('#');
					    writer.append(String.valueOf(Overtime_Cost));
					    writer.append('#');
					    writer.append(String.valueOf(Total_Cost));
					    writer.append('#');
					    writer.append(String.valueOf(Material_Cost));
					    writer.append('#');
					    writer.append(Mach_Name);
					    writer.append('#');
					    writer.append(Mach_No);
					    writer.append('#');
					    writer.append(Mach_Loc);
					    writer.append('#');
					    writer.append(Mach_Type);
					    writer.append('#');
					    writer.append(Name);
					    writer.append('#');
					    writer.append(Trade_Code);
					    writer.append('\n');
					    line_item=0;
					    SQL1b="insert into Bvicosts ([Record Type],[Year],[Month],[Work Record],[Labour Cost],[Overtime Cost]"
								+ ",[Total Cost],[Material Cost],[Asset Name]"
								+ ",[Asset Location],[Asset Type],[Employee],[Employee Name],[Record ID])"
								+ " Values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
					    PreparedStatement preparedStmt = sql_con.prepareStatement(SQL1b);
					    preparedStmt.setString (1,Rec_Type);
					       preparedStmt.setString (2,Year);
					       preparedStmt.setString (3,Month);
					       preparedStmt.setInt (4,Wrec_Id);
					       preparedStmt.setBigDecimal (5,Labour_Cost);
					       preparedStmt.setBigDecimal (6,Overtime_Cost);
					       preparedStmt.setBigDecimal (7,Total_Cost);
					       preparedStmt.setBigDecimal (8,Material_Cost);
					       preparedStmt.setString (9,Mach_Name);
					       preparedStmt.setString (10,Mach_Loc);
					       preparedStmt.setString (11,Mach_Type);
					       preparedStmt.setString (12,Name);
					       preparedStmt.setString (13,Trade_Code);
					       preparedStmt.setInt (14,line_item);
					       preparedStmt.execute();
					}
				
				}
				prog_stmt1a.close();
				prog_stmt1b.close();
				prog_con1.close();
				prog_stmt2.close();
				prog_con2.close();
				sql_stmt.close();	
		    	
		    	sql_con.close();
			}
			 writer.flush();
				writer.close();
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