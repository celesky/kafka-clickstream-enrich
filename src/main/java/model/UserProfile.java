package model;

/**
 * Created by gwen on 1/28/17.
 */
public class UserProfile {
    int userID;
    String userName;
    String zipcode;
    String[] interests;


    public UserProfile(int userID, String userName, String zipcode, String[] interests) {
        this.userID = userID;
        this.userName = userName;
        this.zipcode = zipcode;
        this.interests = interests;
    }

    public int getUserID() {
        return userID;
    }

    public UserProfile update(String zipcode, String[] interests) {
        this.zipcode = zipcode;
        this.interests = interests;
        return this;
    }

}