package gui;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.concurrent.Task;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.TextArea;
import javafx.scene.layout.VBox;
import javafx.stage.FileChooser;
import javafx.stage.Stage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class Task1GUI extends Application {

    private TextArea textArea;

    @Override
    public void start(Stage primaryStage) {
        primaryStage.setTitle("Task 1 - Airline Data");

        textArea = new TextArea();
        textArea.setEditable(false);

        Button loadButton = new Button("Load Airline Data");
        loadButton.setOnAction(e -> chooseFileAndLoadData());

        VBox vbox = new VBox(loadButton, textArea);
        Scene scene = new Scene(vbox, 600, 400);
        primaryStage.setScene(scene);
        primaryStage.show();
    }

    private void chooseFileAndLoadData() {
        FileChooser fileChooser = new FileChooser();
        fileChooser.setTitle("Open Airline Data File");
        File file = fileChooser.showOpenDialog(null);
        if (file != null) {
            loadDataInBackground(file);
        }
    }

    private void loadDataInBackground(File file) {
        Task<Void> task = new Task<Void>() {
            @Override
            protected Void call() throws Exception {
                try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                    String line;
                    StringBuilder sb = new StringBuilder();
                    while ((line = br.readLine()) != null) {
                        sb.append(line).append("\n");
                        if (isCancelled()) {
                            break;
                        }
                    }
                    Platform.runLater(() -> textArea.setText(sb.toString()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return null;
            }
        };
        new Thread(task).start();
    }

    public static void main(String[] args) {
        launch(args);
    }
}
