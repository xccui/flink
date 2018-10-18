/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.examples.java;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JScrollBar;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.ScrollPaneConstants;
import javax.swing.WindowConstants;
import javax.swing.table.DefaultTableModel;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * Simple example for demonstrating the use of the Table API for a Word Count in Java.
 *
 * <p>This example shows how to:
 * - Convert DataSets to Tables
 * - Apply group, aggregate, select, and filter operations
 */
public class WordCountUITable extends JFrame implements ActionListener {

	private enum COMMAND_NAME {
		SELECT, FILTER, REMOVE
	}

	private List<Command> commandList;

	private JTextField commandTextField;
	private JButton addButton1;

	private JButton removeButton1;

	private JTextArea commandTextArea;
	private JScrollPane commandScrollPane;
	private JTable resultTable;
	private JScrollPane resultScrollPane;
	private DefaultTableModel resultModel;


	private StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	private StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

	private Table table;
	private Table actionTable;
	private CsvTableSource tableSource;

	private static final LinkedBlockingQueue<Row> resultQueue = new LinkedBlockingQueue<>();
	private static volatile String currentId = "";

	public WordCountUITable() {
		commandList = Collections.synchronizedList(new LinkedList<>());
		prepareData();
		initUI();
	}

	private void prepareData() {
		tableSource = new CsvTableSource(
				"/Users/xccui/Desktop/mlb_players.csv",
				new String[]{"Name", "Team", "Position", "Height", "Weight", "Age"},
				new TypeInformation[]{Types.STRING(), Types.STRING(), Types.STRING(), Types.INT(), Types.INT(), Types.DOUBLE()},
				", ",
				"\n",
				null,
				true,
				"%%",
				true);
		tEnv.registerTableSource("sample", tableSource);
		table = tEnv.scan("sample");
		actionTable = table;
	}

	private void initUI() {
		this.setSize(800, 600);
		this.getContentPane().setLayout(new FlowLayout());
		this.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);

		commandTextField = new JTextField("");
		commandTextField.setPreferredSize(new Dimension(385, 35));
		addButton1 = new JButton("添加条件");
		addButton1.addActionListener(this);

		removeButton1 = new JButton("删除条件");
		removeButton1.addActionListener(this);

		this.getContentPane().add(commandTextField);
		this.getContentPane().add(addButton1);
		this.getContentPane().add(removeButton1);

		commandTextArea = new JTextArea();
		commandScrollPane = new JScrollPane(commandTextArea);
		commandScrollPane
				.setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_ALWAYS);
		resultTable = new JTable();
		resultScrollPane = new JScrollPane(resultTable);
		resultScrollPane
				.setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_ALWAYS);
		resultScrollPane
				.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS);

		commandScrollPane.setPreferredSize(new Dimension(250, 400));
		commandTextArea.setVisible(true);
		resultScrollPane.setPreferredSize(new Dimension(400, 400));
		resultTable.setVisible(true);

		this.getContentPane().add(commandScrollPane);
		this.getContentPane().add(resultScrollPane);

		new Thread(() -> {
			while (true) {
				Row result;
				try {
					result = resultQueue.take();
					if (result.getField(0).toString().equals(currentId)) {
						Vector<String> v = new Vector<>();
						for (int i = 0; i < result.getArity(); i++) {
							v.add(result.getField(i) == null ? "" : result.getField(i).toString());
						}
						resultModel.addRow(v);
						JScrollBar sb = resultScrollPane.getVerticalScrollBar();
						Thread.sleep(10);
						sb.setValue(sb.getMaximum());
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}).start();
		commandTextArea.setText(actionTable.getSchema().toString());
	}

	private void updateCommandTextArea() {
		commandTextArea.setText("第" + i + "次执行!\n\n");
		commandTextArea.append("当前Schema\n");
		commandTextArea.append(actionTable.getSchema().toString() + "\n");
		for (Command c : commandList) {
			commandTextArea.append(c + "\n");
			commandTextArea.append("===============\n");
		}
	}

	private void changeCommandButtons(boolean enable) {
		addButton1.setEnabled(enable);
		removeButton1.setEnabled(enable);
	}

	private int i = 0;
	private final Object executionLock = new Object();

	private void run() {
		i++;
		currentId = String.valueOf(i);
		actionTable = table;
		for (Command command : commandList) {
			switch (command.commandName) {
				case SELECT:
					actionTable = actionTable.select(command.expression);
					break;
				case FILTER:
					actionTable = actionTable.filter(command.expression);
					break;
				case REMOVE:
					String[] remainingCols =
							Arrays
									.stream(actionTable.getSchema().getColumnNames())
									.filter((value) -> !value.equals(command.expression))
									.toArray(String[]::new);
					String colsStr = Arrays.toString(remainingCols);
					actionTable = actionTable.select(colsStr.substring(1, colsStr.length() - 1));
			}
		}

		Table withIndexTable = actionTable.select(String.valueOf(i) + ", *");

		DataStream<Row> result =
				tEnv.toAppendStream(withIndexTable, withIndexTable.getSchema().toRowType());
		result.addSink(new PrintFunction(currentId)).setParallelism(1);
		Vector<String> cols = new Vector<>();
		cols.add("Index");
		cols.addAll(Arrays.asList(actionTable.getSchema().getColumnNames()));
		resultModel = new DefaultTableModel(cols, 0);
		resultTable.setModel(resultModel);
		new Thread(
				() -> {
					try {
						synchronized (executionLock) {
							env.execute("Job " + i);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
		).start();
		updateCommandTextArea();
		changeCommandButtons(true);
	}

	@Override
	public void actionPerformed(ActionEvent e) {
		changeCommandButtons(false);
		boolean update = false;
		if (e.getSource() == addButton1) {
			String commandStr = commandTextField.getText().trim();
			Command command;
			if (!commandStr.isEmpty()) {
				try {
					command = Command.fromString(commandStr);
					commandList.add(command);
					update = true;
				} catch (Exception e1) {
					e1.printStackTrace();
				}
			}
		} else if (e.getSource() == removeButton1) {
			if (!commandList.isEmpty()) {
				commandList.remove(commandList.size() - 1);
				update = true;
			}
		}
		if (update) {
			try {
				run();
			} catch (Exception e1) {
				e1.printStackTrace();
				commandTextArea.append(e1.getMessage() + "\n");
				if (!commandList.isEmpty()) {
					commandList.remove(commandList.size() - 1);
				}
			} finally {
				changeCommandButtons(true);
			}
		} else {
			changeCommandButtons(true);
		}
	}

	public static void main(String[] args) {
		WordCountUITable uiTable = new WordCountUITable();
		uiTable.setVisible(true);
	}

	/**
	 *
	 */
	public static class PrintFunction extends RichSinkFunction<Row> {

		private String id;

		public PrintFunction(String id) {
			this.id = id;
		}

		@Override
		public void invoke(Row value, Context context) {
			if (id.equals(currentId)) {
				try {
					resultQueue.put(value);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private static class Command {

		private static final String SPLITTER = " ";
		private COMMAND_NAME commandName;
		private String expression;

		public Command(COMMAND_NAME commandName, String expression) {
			this.commandName = commandName;
			this.expression = expression;
		}

		public static Command fromString(String commandStr) throws Exception {
			int splitIndex = commandStr.indexOf(SPLITTER);
			return new Command(
					COMMAND_NAME.valueOf(commandStr.substring(0, splitIndex).toUpperCase()),
					commandStr.substring(splitIndex + 1));
		}

		@Override
		public String toString() {
			return commandName + SPLITTER + expression;
		}
	}
}
