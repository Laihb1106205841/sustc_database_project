package io.sustc.pojo;

import lombok.Getter;

import java.sql.Timestamp;
@Getter
public class VideoMessage {


    /**
     * The BV code of this video
     */
    private String bv;

    /**
     * The title of this video with length >= 1, the video titles of an owner cannot be the same
     */
    private String title;

    /**
     * The owner's {@code mid} of this video
     */
    private long ownerMid;

    /**
     * The owner's {@code name} of this video
     */
    private String ownerName;

    /**
     * The commit time of this video
     */
    private Timestamp commitTime;

    /**
     * The review time of this video, can be null
     */
    private Timestamp reviewTime;

    /**
     * The public time of this video, can be null
     */
    private Timestamp publicTime;
    private Timestamp create_time;

    /**
     * The length in seconds of this video
     */
    private float duration;

    /**
     * The description of this video
     */
    private String description;

    private short is_posted;
    private short is_review;
    private short is_public;
    private Timestamp update_time;

    /**
     * The reviewer of this video, can be null
     */
    private Long reviewer;
    public VideoMessage(String bv, String title,long ownerMid,String ownerName,Timestamp create_time,
                        Timestamp commitTime,Timestamp reviewTime,Timestamp publicTime,float duration,
                        String description,Long reviewer,Timestamp update_time,short is_posted,short is_review,short is_public){

        this.bv=bv;
        this.title=title;
        this.ownerMid=ownerMid;
        this.ownerName=ownerName;
        this.create_time=create_time;
        this.commitTime=commitTime;
        this.reviewTime=reviewTime;
        this.publicTime=publicTime;
        this.duration=duration;
        this.description=description;
        this.reviewer=reviewer;
        this.is_public=is_public;
        this.is_review=is_review;
        this.is_posted=is_posted;
        this.update_time=update_time;

    }
}
