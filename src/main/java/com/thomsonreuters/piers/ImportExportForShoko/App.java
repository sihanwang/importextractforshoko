package com.thomsonreuters.piers.ImportExportForShoko;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class App {

	/**
	 * @param tl
	 *            要生成csv文件的table名字列表
	 * @return 生成的文件的路径列表
	 * @throws Exception
	 */
	public List<String> startTableToCSV(String sql, String filename) throws Exception {
		List<String> fileList = new ArrayList<String>();
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = new DBConnections(DBConnections.URL, DBConnections.USERNAME, DBConnections.PASSWORD).getConn();
			conn.setAutoCommit(false);
			stmt = conn.createStatement();

			int count = 0;

			File file = createEmptyFile(filename);
			FileWriter fw = new FileWriter(file, true);

			ResultSet rs = stmt.executeQuery(sql);
			writeToFile(fw, rs, ++count);
			fw.close();
			fileList.add(file.getAbsolutePath());

			stmt.close();
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
		return fileList;
	}

	// 将文件数据库记录写入文件
	private void writeToFile(FileWriter fw, ResultSet rs, int count) throws Exception {
		try {
			ResultSetMetaData rd = rs.getMetaData();
			int fields = rd.getColumnCount();
			if (rd.getColumnName(fields).equals("RN")) {
				fields--;
			}
			if (count == 1) {
				for (int i = 1; i <= fields; i++) {
					fw.write(rd.getColumnName(i));
					if (i == fields)
						fw.write("\n");
					else
						fw.write(",");
				}
				fw.flush();
			}
			writeToFile(fields, fw, rs);
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
	}

	// 将数据记录写入文件
	private void writeToFile(int fields, FileWriter fw, ResultSet rs) throws Exception {
		try {
			while (rs.next()) {
				for (int i = 1; i <= fields; i++) {
					String temp = rs.getString(i);
					if (!rs.wasNull()) {
						// 这里将记录里面的特殊符号进行替换， 假定数据中不包含替换后的特殊字串
						temp = temp.replaceAll(",", "&%&");
						temp = temp.replaceAll("\n\r|\r|\n|\r\n", "&#&");
						fw.write(temp);
					}
					if (i == fields)
						fw.write("\r\n");
					else
						fw.write(",");
				}

				fw.flush();
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
	}

	// 创建一个空文件
	private File createEmptyFile(String filename) throws Exception {

		File file = new File(filename);
		try {
			if (file.exists()) {
				file.delete();
				file.createNewFile();
			} else {
				file.createNewFile();
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
		return file;
	}

	private String generateFilename(String t) {
		String filename = "";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		filename += t;
		filename += "_";
		filename += sdf.format(new Date());
		filename += ".csv";
		return filename;
	}

	// 测试
	public static void main(String args[]) {

		try {			
			
			
			String SqlToExtractImport="SELECT ptr.id ptr_id,"
					+" ptr.recnum piers_record_number,"
					+" ptr.dir,"
					+" ptr.vdate,"
					+" ptr.pdate piers_processing_date,"
					+" ptr.udate piers_update_date,"
					+" ptr.commodity_id,"
					+" (SELECT pco.NAME commodity FROM piers_commodity pco WHERE ptr.commodity_id = pco.id),"
					+" (SELECT pco.com7_code FROM piers_commodity pco WHERE ptr.commodity_id = pco.id),"
					+" (SELECT pco.com7_desc FROM piers_commodity pco WHERE ptr.commodity_id = pco.id),"
					+" (SELECT pco.com4_code FROM piers_commodity pco WHERE ptr.commodity_id = pco.id),"
					+" (SELECT pco.com4_desc FROM piers_commodity pco WHERE ptr.commodity_id = pco.id),"
					+" (SELECT pco.hs_code harm_code FROM piers_commodity pco WHERE ptr.commodity_id = pco.id),"
					+" (SELECT pco.harm_desc FROM piers_commodity pco WHERE ptr.commodity_id = pco.id),"
					+" (SELECT pco.harm4_code FROM piers_commodity pco WHERE ptr.commodity_id = pco.id),"
					+" (SELECT pco.h4_desc harm4_desc FROM piers_commodity pco WHERE ptr.commodity_id = pco.id),"
					+" ptr.ultctry_id,"
					+" (SELECT pge_ultctry.code ult_ctrycode FROM piers_geography pge_ultctry WHERE ptr.ultctry_id = pge_ultctry.id),"
					+" (SELECT pge_ultctry.NAME ult_ctryname FROM piers_geography pge_ultctry WHERE ptr.ultctry_id = pge_ultctry.id),"
					+" (SELECT pge_ultctry.longname ult_ctryname_l FROM piers_geography pge_ultctry WHERE ptr.ultctry_id = pge_ultctry.id),"
					+" ptr.ultport_id,"
					+" (SELECT pge_ultport.code ultcode FROM piers_geography pge_ultport WHERE ptr.ultport_id = pge_ultport.id),"
					+" (SELECT pge_ultport.NAME ultport FROM piers_geography pge_ultport WHERE ptr.ultport_id = pge_ultport.id),"
					+" ptr.usport_id,"
					+" (SELECT pge_usport.code uscode FROM piers_geography pge_usport WHERE ptr.usport_id = pge_usport.id),"
					+" (SELECT pge_usport.NAME usport FROM piers_geography pge_usport WHERE ptr.usport_id = pge_usport.id),"
					+" ptr.fport_id,"
					+" (SELECT pge_fport.code fcode FROM piers_geography pge_fport WHERE ptr.fport_id = pge_fport.id),"
					+" (SELECT pge_fport.NAME fport FROM piers_geography pge_fport WHERE ptr.fport_id = pge_fport.id),"
					+" (SELECT pge_usib_city.code usib_city_code FROM piers_geography pge_usib_city WHERE ptr.usib_city_id = pge_usib_city.id),"
					+" (SELECT pge_usib_city.NAME usib_city FROM piers_geography pge_usib_city WHERE ptr.usib_city_id = pge_usib_city.id),"
					+" (SELECT pge_usib_state.code usib_state FROM piers_geography pge_usib_state WHERE ptr.usib_state_id = pge_usib_state.id),"
					+" (SELECT pge_fgnib_city.code fgnib_city_code FROM piers_geography pge_fgnib_city WHERE ptr.fgnib_city_id = pge_fgnib_city.id),"
					+" (SELECT pge_fgnib_city.NAME fgnib_city FROM piers_geography pge_fgnib_city WHERE ptr.fgnib_city_id = pge_fgnib_city.id),"
					+" (SELECT pge_fgnib_ctry.NAME fgnib_ctry FROM piers_geography pge_fgnib_ctry WHERE ptr.fgnib_ctry_id = pge_fgnib_ctry.id),"
					+" (SELECT pge_org_des_city.NAME org_des_city FROM piers_geography pge_org_des_city WHERE ptr.org_des_city_id = pge_org_des_city.id),"
					+" (SELECT pge_org_des_ctry.NAME org_des_state FROM  piers_geography pge_org_des_ctry WHERE ptr.org_des_ctry_id = pge_org_des_ctry.id) ,"
					+" ptr.consignee_id,"
					+" ppa_consignee.NAME consignee_name,"
					+" ppa_consignee.city consignee_city,"
					+" ppa_consignee.state consignee_state,"
					+" ppa_consignee.street consignee_street,"
					+" ppa_consignee.street2 consignee_street2,"
					+" ppa_consignee.zipcode consignee_zipcode,"
					+" ppa_consignee.comp_nbr consignee_comp_nbr,"
					+" ppa_consignee.main_cmp_nbr consignee_main_comp_nbr,"
					+" ptr.shipper_id,"
					+" ppa_shipper.NAME shipper_name,"
					+" ppa_shipper.city shipper_city,"
					+" ppa_shipper.ctrycode shipper_ctrycode,"
					+" ppa_shipper.country shipper_country,"
					+" ppa_shipper.countryl shipper_countryl,"
					+" ppa_shipper.street shipper_street,"
					+" ppa_shipper.street2 shipper_street2,"
					+" ppa_shipper.zipcode shipper_zipcode,"
					+" ppa_shipper.comp_nbr shipper_comp_nbr,"
					+" ppa_shipper.main_cmp_nbr shipper_main_comp_nbr,"
					+" ptr.nofity_party_id notify_id,"
					+" ppa_nofity.NAME notify_name,"
					+" ppa_nofity.city notify_city,"
					+" ppa_nofity.state notify_state,"
					+" ppa_nofity.street notify_street,"
					+" ppa_nofity.street2 notify_street2,"
					+" ppa_nofity.zipcode notify_zipcode,"
					+" ppa_nofity.comp_nbr notify_comp_nbr,"
					+" ptr.vessel_id,"
					+" (SELECT pve.NAME vessel FROM piers_vessel pve WHERE ptr.vessel_id = pve.id),"
					+" (SELECT pve.imo vessel_code FROM piers_vessel pve WHERE ptr.vessel_id = pve.id),"
					+" (SELECT pve.registry vessel_registry FROM piers_vessel pve WHERE ptr.vessel_id = pve.id),"
					+" ptr.sline,"
					+" ptr.voyage,"
					+" ptr.manifest_nbr,"
					+" ptr.bol_nbr,"
					+" ptr.quantity,"
					+" ptr.mea_unit measurement_unit,"
					+" ptr.reefer,"
					+" ptr.roro,"
					+" ptr.hazmat,"
					+" ptr.nvocc,"
					+" ptr.conflag containerized_indicator,"
					+" ptr.consize container_size,"
					+" ptr.conqty container_qty,"
					+" ptr.convol container_volume,"
					+" ptr.teus,"
					+" ptr.feus,"
					+" ptr.pounds,"
					+" ptr.mtons,"
					+" ptr.ltons,"
					+" ptr.stons,"
					+" ptr.kilos,"
					+" ptr.shipments,"
					+" ptr.financial financial_indicator,"
					+" ppa_bank.NAME bank_name,"
					+" ptr.payable,"
					+" ptr.VALUE estimated_cargo_value,"
					+" ptr.raw_commodity"
					+" FROM (SELECT /*+ use_nl(prt1,cfl)index(cfl cfl_IDX1 ) */"
					+" *"
					+" FROM piers_transaction ptr1"
					+" WHERE ptr1.pounds > 0"
					+" AND ptr1.dir = 'I'"
					+" AND ptr1.vdate BETWEEN to_date('2017-04-29', 'YYYY-MM-DD') AND"
					+" to_date('2017-04-30', 'YYYY-MM-DD')"
					+" AND NOT EXISTS (SELECT /*+ use_nl(prt,cfl)index(cfl cfl_IDX1 ) */"
					+" 1"
					+" FROM commodity_flow_link cfl"
					+" WHERE cfl.object_type = 'PIERS'"
					+" AND cfl.object_id = ptr1.id)"
					+" AND EXISTS (SELECT 1"
					+" FROM piers_geography pge_fport"
					+" WHERE ptr1.fport_id = pge_fport.id)"
					+" AND EXISTS (SELECT 1"
					+" FROM piers_geography pge_usport"
					+" WHERE ptr1.usport_id = pge_usport.id)"
					+" AND ptr1.last_updated_by IS NOT NULL) ptr,"
					+" "
					+" piers_party ppa_consignee,"
					+" piers_party ppa_shipper,"
					+" piers_party ppa_nofity,"
					+" piers_party ppa_bank"
					+" WHERE ptr.consignee_id = ppa_consignee.id(+)"
					+" AND ptr.shipper_id = ppa_shipper.id(+)"
					+" AND ptr.nofity_party_id = ppa_nofity.id(+)"
					+" AND ptr.bank_id = ppa_bank.id(+)";
					   


			System.out.println(new App().startTableToCSV(SqlToExtractImport,"c:\\temp\\ImportBetween.20170429-20170430.csv"));

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}