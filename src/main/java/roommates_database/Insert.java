package com.roommates_database.roommates_database;

import org.json.*;

import java.sql.*;
import java.io.*;
import java.util.*;
import java.text.*;
import java.net.*;

public class Insert {
    static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost/roommates";

    public static void insertData() {
        Connection conn = null;
        PreparedStatement ps = null;

        try {
            File params_file = new File("../src/main/java/roommates_database/configs/dbparams.txt");
            File data_file = new File("../src/main/java/roommates_database/data/data.json");
            File desired_file = new File("../src/main/java/roommates_database/data/desired_data.json");
            Scanner sc = new Scanner(params_file);
            final String USER = sc.nextLine();
            final String PASSWORD = sc.nextLine();
            sc.close();

            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(DB_URL, USER, PASSWORD);
            conn.setAutoCommit(false);

            JSONTokener data_tokener = new JSONTokener(new FileReader(data_file));
            JSONTokener desired_data_tokener = new JSONTokener(new FileReader(desired_file));

            JSONObject data = new JSONObject(data_tokener);
            JSONObject desired_data = new JSONObject(desired_data_tokener);
            JSONArray data_array = data.getJSONArray("results");
            JSONArray desired_data_array = data.getJSONArray("results");

            String insertTableSQL = "INSERT INTO Users (email, password, first_name, last_name, gender, zipcode, birthdate, description, picture, sleep, eat, neat, social, desired_zipcode, desired_gender, desired_rent) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            ps = conn.prepareStatement(insertTableSQL);

            int batchSize = 100;
            for (int i = 0; i < data_array.length(); i++) {
                JSONObject temp = data_array.getJSONObject(i);
                JSONObject desired_temp = desired_data_array.getJSONObject(i);
                JSONTokener description_tokener = new JSONTokener(getHTML("https://api.kanye.rest/"));
                JSONObject description_json = new JSONObject(description_tokener);
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                java.util.Date date = format.parse(temp.getString("dob"));
                java.sql.Date sqlDate = new java.sql.Date(date.getTime());

                ps.setString(1, temp.getString("email"));
                ps.setString(2, temp.getString("sha256"));
                ps.setString(3, temp.getString("first"));
                ps.setString(4, temp.getString("last"));
                ps.setString(5, temp.getString("gender").substring(0, 1));
                ps.setString(6, Integer.toString(temp.getInt("postcode")));
                ps.setDate(7, sqlDate);
                ps.setString(8, description_json.getString("quote"));
                ps.setString(9, temp.getString("picture"));
                ps.setInt(10, (int) (Math.random()*10));
                ps.setInt(11, (int) (Math.random()*10));
                ps.setInt(12, (int) (Math.random()*10));
                ps.setInt(13, (int) (Math.random()*10));
                ps.setString(14, Integer.toString(desired_temp.getInt("postcode")));
                ps.setString(15, desired_temp.getString("gender").substring(0, 1));
                ps.setInt(16, ((int) (Math.random() * (3000 - 100)) + 100) / 100 * 100);

                ps.addBatch();
                if (i % batchSize == 0) {
                    ps.executeBatch();
                    System.out.println(i);
                }
            }

            ps.executeBatch();
            conn.commit();
            ps.close();
            conn.close();
        } catch(SQLException e) {
            e.printStackTrace();
        } catch(ClassNotFoundException e) {
            e.printStackTrace();
        } catch(FileNotFoundException e) {
            e.printStackTrace();
        } catch(JSONException e) {
            e.printStackTrace();
        } catch(org.json.simple.parser.ParseException e) {
            e.printStackTrace();
        } catch(java.text.ParseException e) {
            e.printStackTrace();
        } catch(IOException e) {
            e.printStackTrace();
        } catch(java.lang.Exception e) {
            e.printStackTrace();
        }
    }

    private static String getHTML(String urlToRead) throws Exception {
        StringBuilder result = new StringBuilder();
        URL url = new URL(urlToRead);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        String line;

        while ((line = rd.readLine()) != null) {
            result.append(line);
        }

        rd.close();
        return result.toString();
    }

    public static void main(String[] args) {
        insertData();
    }
}
