package com.adc.da.bean;

import java.util.Arrays;
import java.util.Objects;

/**
 * 原始数据
 */
public class Data {
    private String vin;
    private String msgTime;// 报文时间
    private Float speed;
    private String startupStatus;// 1 表示启动，0表示熄火 启动状态
    private String runMode;//1表示纯电  运行模式
    private Float odo;// 累计里程
    private String gearStatus;//1 表示自动挡， 0表示停车  档位状态
    private String chargeStatus;//0 未充电状态 1 表示停车状态  充电状态
    private Float aptv;// 油门踏板行程
    private Float bptv;// 刹车踏板状态
    private Float totalVoltage; // 总电压
    private Float totalCurrent; //总电流
    private String soc; //SOC
    private Float insulationResistance; // 绝缘电阻
    private String positionStatus; // 1 表示有效 0 表示无效  定位状态
    private Float longitude; // 维度
    private Float latitude;  // 经度
    private String failure;// 故障码
    private Integer cellNum;// 单体数量
    private Integer probeNum;// 探针数量
    private Float[] cellVoltage;// 单体电压
    private Float[] probeTemperature;// 探针温度

    @Override
    public String toString() {
        return "Data{" +
                "vin='" + vin + '\'' +
                ", msgTime='" + msgTime + '\'' +
                ", speed=" + speed +
                ", startupStatus='" + startupStatus + '\'' +
                ", runMode='" + runMode + '\'' +
                ", odo=" + odo +
                ", gearStatus='" + gearStatus + '\'' +
                ", chargeStatus='" + chargeStatus + '\'' +
                ", aptv=" + aptv +
                ", bptv=" + bptv +
                ", totalVoltage=" + totalVoltage +
                ", totalCurrent=" + totalCurrent +
                ", SOC='" + soc + '\'' +
                ", insulationResistance=" + insulationResistance +
                ", positionStatus='" + positionStatus + '\'' +
                ", longitude=" + longitude +
                ", latitude=" + latitude +
                ", failure='" + failure + '\'' +
                ", cellNum=" + cellNum +
                ", probeNum=" + probeNum +
                ", cellVoltage=" + Arrays.toString(cellVoltage) +
                ", probeTemperature=" + Arrays.toString(probeTemperature) +
                '}';
    }

    public String getVin() {
        return vin;
    }

    public void setVin(String vin) {
        this.vin = vin;
    }

    public String getMsgTime() {
        return msgTime;
    }

    public void setMsgTime(String msgTime) {
        this.msgTime = msgTime;
    }

    public Float getSpeed() {
        return speed;
    }

    public void setSpeed(Float speed) {
        this.speed = speed;
    }

    public String getStartupStatus() {
        return startupStatus;
    }

    public void setStartupStatus(String startupStatus) {
        this.startupStatus = startupStatus;
    }

    public String getRunMode() {
        return runMode;
    }

    public void setRunMode(String runMode) {
        this.runMode = runMode;
    }

    public Float getOdo() {
        return odo;
    }

    public void setOdo(Float odo) {
        this.odo = odo;
    }

    public String getGearStatus() {
        return gearStatus;
    }

    public void setGearStatus(String gearStatus) {
        this.gearStatus = gearStatus;
    }

    public String getChargeStatus() {
        return chargeStatus;
    }

    public void setChargeStatus(String chargeStatus) {
        this.chargeStatus = chargeStatus;
    }

    public Float getAptv() {
        return aptv;
    }

    public void setAptv(Float aptv) {
        this.aptv = aptv;
    }

    public Float getBptv() {
        return bptv;
    }

    public void setBptv(Float bptv) {
        this.bptv = bptv;
    }

    public Float getTotalVoltage() {
        return totalVoltage;
    }

    public void setTotalVoltage(Float totalVoltage) {
        this.totalVoltage = totalVoltage;
    }

    public Float getTotalCurrent() {
        return totalCurrent;
    }

    public void setTotalCurrent(Float totalCurrent) {
        this.totalCurrent = totalCurrent;
    }

    public String getSoc() {
        return soc;
    }

    public void setSoc(String soc) {
        this.soc = soc;
    }

    public Float getInsulationResistance() {
        return insulationResistance;
    }

    public void setInsulationResistance(Float insulationResistance) {
        this.insulationResistance = insulationResistance;
    }

    public String getPositionStatus() {
        return positionStatus;
    }

    public void setPositionStatus(String positionStatus) {
        this.positionStatus = positionStatus;
    }

    public Float getLongitude() {
        return longitude;
    }

    public void setLongitude(Float longitude) {
        this.longitude = longitude;
    }

    public Float getLatitude() {
        return latitude;
    }

    public void setLatitude(Float latitude) {
        this.latitude = latitude;
    }

    public String getFailure() {
        return failure;
    }

    public void setFailure(String failure) {
        this.failure = failure;
    }

    public Integer getCellNum() {
        return cellNum;
    }

    public void setCellNum(Integer cellNum) {
        this.cellNum = cellNum;
    }

    public Integer getProbeNum() {
        return probeNum;
    }

    public void setProbeNum(Integer probeNum) {
        this.probeNum = probeNum;
    }

    public Float[] getCellVoltage() {
        return cellVoltage;
    }

    public void setCellVoltage(Float[] cellVoltage) {
        this.cellVoltage = cellVoltage;
    }

    public Float[] getProbeTemperature() {
        return probeTemperature;
    }

    public void setProbeTemperature(Float[] probeTemperature) {
        this.probeTemperature = probeTemperature;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Data data = (Data) o;
        return Objects.equals(vin, data.vin) &&
                Objects.equals(msgTime, data.msgTime) &&
                Objects.equals(speed, data.speed) &&
                Objects.equals(startupStatus, data.startupStatus) &&
                Objects.equals(runMode, data.runMode) &&
                Objects.equals(odo, data.odo) &&
                Objects.equals(gearStatus, data.gearStatus) &&
                Objects.equals(chargeStatus, data.chargeStatus) &&
                Objects.equals(aptv, data.aptv) &&
                Objects.equals(bptv, data.bptv) &&
                Objects.equals(totalVoltage, data.totalVoltage) &&
                Objects.equals(totalCurrent, data.totalCurrent) &&
                Objects.equals(soc, data.soc) &&
                Objects.equals(insulationResistance, data.insulationResistance) &&
                Objects.equals(positionStatus, data.positionStatus) &&
                Objects.equals(longitude, data.longitude) &&
                Objects.equals(latitude, data.latitude) &&
                Objects.equals(failure, data.failure) &&
                Objects.equals(cellNum, data.cellNum) &&
                Objects.equals(probeNum, data.probeNum) &&
                Arrays.equals(cellVoltage, data.cellVoltage) &&
                Arrays.equals(probeTemperature, data.probeTemperature);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(vin, msgTime, speed, startupStatus, runMode, odo, gearStatus, chargeStatus, aptv, bptv, totalVoltage, totalCurrent, soc, insulationResistance, positionStatus, longitude, latitude, failure, cellNum, probeNum);
        result = 31 * result + Arrays.hashCode(cellVoltage);
        result = 31 * result + Arrays.hashCode(probeTemperature);
        return result;
    }
}
