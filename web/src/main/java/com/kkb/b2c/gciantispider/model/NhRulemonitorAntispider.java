package com.kkb.b2c.gciantispider.model;

import javax.persistence.*;
import java.sql.Timestamp;

import static javax.persistence.GenerationType.IDENTITY;

/**
 * NhRulemonitorAntispider entity. @author MyEclipse Persistence Tools
 */
@Entity
@Table(name = "nh_rulemonitor_antispider", catalog = "gciantispider")
public class NhRulemonitorAntispider implements java.io.Serializable {

    // Fields

    private Integer id;

    private Timestamp datetime;

    private Integer timetype;

    private Integer ruletype;

    private Integer numx;

    private Integer numy;

    // Constructors

    /** default constructor */
    public NhRulemonitorAntispider() {
    }

    /** full constructor */
    public NhRulemonitorAntispider(Timestamp datetime, Integer timetype, Integer ruletype, Integer numx, Integer numy) {
        this.datetime = datetime;
        this.timetype = timetype;
        this.ruletype = ruletype;
        this.numx = numx;
        this.numy = numy;
    }

    // Property accessors
    @Id
    @GeneratedValue(strategy = IDENTITY)
    @Column(name = "id", unique = true, nullable = false)
    public Integer getId() {
        return this.id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    @Column(name = "datetime", length = 19)
    public Timestamp getDatetime() {
        return this.datetime;
    }

    public void setDatetime(Timestamp datetime) {
        this.datetime = datetime;
    }

    @Column(name = "timetype")
    public Integer getTimetype() {
        return this.timetype;
    }

    public void setTimetype(Integer timetype) {
        this.timetype = timetype;
    }

    @Column(name = "ruletype")
    public Integer getRuletype() {
        return this.ruletype;
    }

    public void setRuletype(Integer ruletype) {
        this.ruletype = ruletype;
    }

    @Column(name = "numx")
    public Integer getNumx() {
        return this.numx;
    }

    public void setNumx(Integer numx) {
        this.numx = numx;
    }

    @Column(name = "numy")
    public Integer getNumy() {
        return this.numy;
    }

    public void setNumy(Integer numy) {
        this.numy = numy;
    }

}