package gui;

import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextArea;
import javafx.scene.layout.VBox;
import javafx.stage.FileChooser;
import javafx.stage.Stage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class MainClass extends Application {

    private TextArea sentimentTextArea;

    public static void main(String[] args) {
        launch(args);
    }

    @Override
    public void start(Stage primaryStage) {
        primaryStage.setTitle("Twitter Sentiment Analysis");

        Label label = new Label("Negative Sentiments:");
        sentimentTextArea = new TextArea();
        sentimentTextArea.setEditable(false);

        Button loadButton = new Button("Load Sentiments");
        loadButton.setOnAction(e -> loadSentiments());

        VBox vbox = new VBox(label, sentimentTextArea, loadButton);
        Scene scene = new Scene(vbox, 400, 300);

        primaryStage.setScene(scene);
        primaryStage.show();
    }

    private void loadSentiments() {
        FileChooser fileChooser = new FileChooser();
        fileChooser.setTitle("Open Sentiments File");
        File file = fileChooser.showOpenDialog(null);
        if (file != null) {
            try {
                BufferedReader br = new BufferedReader(new FileReader(file));
                String line;
                StringBuilder sb = new StringBuilder();
                while ((line = br.readLine()) != null) {
                    String[] columns = line.split(",");
                    if (columns.length > 1 && "negative".equals(columns[1].trim())) {
                        sb.append(columns[0]).append(": ").append(columns[2]).append("\n");
                    }
                }
                br.close();
                sentimentTextArea.setText(sb.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
