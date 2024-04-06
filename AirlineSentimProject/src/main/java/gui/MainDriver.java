package gui;

import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;

public class MainDriver extends Application {

    public static void main(String[] args) {
        launch(args);
    }

    @Override
    public void start(Stage primaryStage) {
        primaryStage.setTitle("Twitter Sentiment Analysis");

        Button task1Button = new Button("Open Task 1 GUI");
        task1Button.setOnAction(e -> openTask1GUI());

        Button task2Button = new Button("Open Task 2 GUI");
        task2Button.setOnAction(e -> openTask2GUI());

        // Add buttons for other tasks similarly...

        VBox vbox = new VBox(task1Button, task2Button /* Add other buttons here */);
        Scene scene = new Scene(vbox, 200, 200);

        primaryStage.setScene(scene);
        primaryStage.show();
    }

    private void openTask1GUI() {
        Task1GUI task1GUI = new Task1GUI();
        task1GUI.start(new Stage());
    }

    private void openTask2GUI() {
        Task2GUI task2GUI = new Task2GUI();
        task2GUI.start(new Stage());
    }

    // Add separate functions for other tasks similarly...
}
