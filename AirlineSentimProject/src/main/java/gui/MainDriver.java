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

        Button task1Button = new Button("Task 1");
        task1Button.setOnAction(e -> openTask1GUI());

        Button task2Button = new Button("Task 2");
        task2Button.setOnAction(e -> openTask2GUI());

        Button task3Button = new Button("Task 3");
        task3Button.setOnAction(e -> openTask3GUI());

        Button task4Button = new Button("Task 4");
        task4Button.setOnAction(e -> openTask4GUI());

        Button task5Button = new Button("Task 5");
        task5Button.setOnAction(e -> openTask5GUI());

        Button task6Button = new Button("Task 6");
        task6Button.setOnAction(e -> openTask6GUI());

        VBox vbox = new VBox(task1Button, task2Button, task3Button, task4Button, task5Button, task6Button);
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

    private void openTask3GUI() {
        Task3GUI task3GUI = new Task3GUI();
        task3GUI.start(new Stage());
    }

    private void openTask4GUI() {
        Task4GUI task4GUI = new Task4GUI();
        task4GUI.start(new Stage());
    }

    private void openTask5GUI() {
        Task5GUI task5GUI = new Task5GUI();
        task5GUI.start(new Stage());
    }

    private void openTask6GUI() {
        Task6GUI task6GUI = new Task6GUI();
        task6GUI.start(new Stage());
    }
}
