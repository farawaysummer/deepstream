package com.rui.dp.prj;

import com.rui.dp.prj.job.DeepStreamHelper;
import com.rui.ds.datasource.DatabaseSources;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.types.Row;

import java.sql.*;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A simple asynchronous client that simulates interacting with an unreliable external service.
 */
public class AsyncClient {

    public AsyncClient() {
        DeepStreamHelper.loadDatasource();
        DeepStreamHelper.loadSql();
    }

    public CompletableFuture<Collection<Row>> query(Row key) {
        return CompletableFuture.supplyAsync(
                () -> {
                    return queryDB(key);
                });
    }

    public List<Row> queryDB(Row row) {
        try (Connection connection = DatabaseSources.getConnection("HIS")) {
            String sql = DeepStreamHelper.getSql("selectHIS");
            List<Row> rows = Lists.newArrayList();

            PreparedStatement statement = connection.prepareStatement(sql);
            //AND A.NO= ? AND A.KFDM = ? AND A.LX = ? AND A.SFNO = ?
            Object NO = row.getField("NO");
            Object KFDM = row.getField("KFDM");
            Object LX = row.getField("LX");
            Object SFNO = row.getField("SFNO");

            statement.setObject(1, NO);
            statement.setObject(2, KFDM);
            statement.setObject(3, LX);
            statement.setObject(4, SFNO);

            System.out.println("====Start execute query");
            ResultSet result = statement.executeQuery();
            while (result.next()) {
                String ORG_CODE = result.getString(1);
                Row newRow = Row.join(row, Row.of(ORG_CODE));
                rows.add(newRow);
            }
            System.out.println("====Finish execute query with result:" + rows);
            return rows;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }
}